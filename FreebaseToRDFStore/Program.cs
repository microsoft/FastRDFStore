using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using CommandLine;

namespace FreebaseToRDFStore
{
    internal enum Command
    {
        TrimData,
        BuildStore,
        FindGhost
    }

    internal class CommandLineArguments
    {
        [Option('c', "cmd", Required = true,
            HelpText = "Run TrimData first, then BuildStore, then FindGhost. TrimData trims the raw freebase RDF dump and outputs the filtered fb_en*. " +
                       "BuildStore reads the filtered triples fb_en* and outputs the in-memory store (*.bin file). " +
                       "Use FindGhost (find ghost object nodes) in the end.",
            DefaultValue = Command.BuildStore)]
        public Command cmd { get; set; }

        [Option('i', "idir", HelpText = "Input directory for reading files", DefaultValue = "")]
        public string idir { get; set; }

        [Option('o', "odir", HelpText = "Output directory for writing files", DefaultValue = "")]
        public string odir { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return CommandLine.Text.HelpText.AutoBuild(this,
              (CommandLine.Text.HelpText current) =>
              {

                  current.Copyright = " ";
                  current.AdditionalNewLineAfterOption = false;
                  current.MaximumDisplayWidth = Console.WindowWidth;
                  current.Heading = System.AppDomain.CurrentDomain.FriendlyName + " Usage:";
                  CommandLine.Text.HelpText.DefaultParsingErrorsHandler(this, current);
              });
        }

    }

    internal class Program
    {
        private const int maxBuilderLengthInChars = 250*1024*1024; // 500MB (bytes not chars)

        private static void Main(string[] args)
        {
            CommandLineArguments cmd = new CommandLineArguments();
            Parser.Default.ParseArgumentsStrict(args, cmd);

            if (cmd.cmd == Command.TrimData)
                TrimData(cmd);
            else if (cmd.cmd == Command.BuildStore)
            {
                BuildStore(cmd);
            }
            else if (cmd.cmd == Command.FindGhost)
            {
                FindGhost(cmd);
            }

            Console.WriteLine("Done");
        }

        public static void BuildStore(CommandLineArguments cmd)
        {
            string midToOffsetFilename = Path.Combine(cmd.odir, "midToOffset.bin");
            string largeMidToOffsetFilename = Path.Combine(cmd.odir, "largeMidToOffset.bin");
            string datapagesFilename = Path.Combine(cmd.odir, "datapages.bin");
            string cvtNodesFilename = Path.Combine(cmd.odir, "cvtnodes.bin");
            string namesTableFilename = Path.Combine(cmd.odir, "namesTable.bin");

            // Ensure we can write to the output files before starting
            (new StreamWriter(midToOffsetFilename)).Close();
            (new StreamWriter(largeMidToOffsetFilename)).Close();
            (new StreamWriter(datapagesFilename)).Close();
            (new StreamWriter(cvtNodesFilename)).Close();
            (new StreamWriter(namesTableFilename)).Close();

            var ifile1 = Path.Combine(cmd.idir, "fb_en.txt");
            var ifile2 = Path.Combine(cmd.idir, "fb_en_nonM.txt");

            // SY: This is the main data structure that recrods the data location. 
            //     Basically, given an subject MID, it records where in the binary (compressed) file contains the group of tuples. 
            //     key -> { (subject, (start_position, length) }
            //     key is some prefix of the subject MID, designed for breaking a large group (some MID subject has a lot of tuples)
            //     subject: the MID of the subject
            //     start_position: the starting position of the group in the binary (compressed) file.
            //     length: the length of the group in the binary (compressed) file
            var midToOffsetDictionaries = new Dictionary<string, Dictionary<string, Tuple<long, int>>>();

            // SY: If an MID group is too large and has been broken into several parts, then this dictionary stores the starting position, and the size of each part
            //     subject -> (start_position, [size1, size2, size3, ...])
            var largeMidsToCompressedBlobsLocations = new Dictionary<string, Tuple<long, List<int>>>();

            // SY: A table to store whether a subject is a CVT node or not.  The data structure is similar to midToOffsetDictionaries, although I think a regular Dictionary<string, bool> should work fine.
            //     key -> { (subject, isCVT }
            var cvtNodes = new Dictionary<string, Dictionary<string, bool>>();

            // SY: The table to store entity names.
            Dictionary<string, string> namesTable = new Dictionary<string, string>();

            long currentOffset = 0;
            long curLines = 0; // counter for the number of lines
            var currentMidCounts = 0.0;
            var avgUncompressedSize = 0.0;
            var avgCompressedSize = 0.0;

            // SY: Predicates that will be excluded from the index.
            var excludedDomains = new[] {"authority", "imdb", "internet", "source"}.Select(x => x + ".");
            var excludedPredicates = new[]
            {
                "type.object.key",
                "type.object.permission",
                "common.topic.image",

                "common.topic.topic_equivalent_webpage",
                "common.topic.topical_webpage",
                "en",
                "base.ranker.rankerurlname",

                "type.object.type",
                "common.topic.description"
            };


            using (var datapagesWriter = new BinaryWriter(File.OpenWrite(datapagesFilename)))
            {
                var lastSubject = "";
                var builder = new StringBuilder();
                var multipleCompressedLengths = new List<int>();
                long totalRawBytes = 0;
                bool wroteMidToConsole = false;
                bool isCVT = true;

                // SY: Adding the dummy line in the end to make sure that the final group is indexed.
                foreach (var line in File.ReadLines(ifile1).Concat(File.ReadLines(ifile2)).Concat(new string[] {"dummy\tdummy\tdummy"}))
                {
                    try
                    {
                        curLines++;
                        var parts = line.Split('\t');

                        if (excludedPredicates.Contains(parts[1]) || excludedDomains.Any(pre => parts[1].StartsWith(pre)))
                            continue;

                        // SY: Assuming the tuples are grouped by the MIDs of the subject field
                        var subject = parts[0];
                        if (subject == lastSubject || lastSubject == "") // SY: still the same group of tuples, or just the first line
                        {
                            // SY: Append the predicate and object to the string builder (for this subject)
                            builder.Append(parts[1]);
                            builder.Append("\t");
                            builder.Append(parts[2]);
                            builder.AppendLine();

                            // SY: Use the existence of the entity name as the indication for whether this subject is a CVT 
                            if (parts[1] == "type.object.name")
                            {
                                isCVT = false;
                                // SY: if an entity has more than one name, this table will only store the last one.
                                namesTable[parts[0]] = parts[2];
                            }

                            // SY: If this group is too large, break it
                            if (builder.Length > maxBuilderLengthInChars)
                            {
                                long rawBytes;
                                var compressedBytesCount = CompressAndSave(builder, datapagesWriter, out rawBytes);
                                multipleCompressedLengths.Add(compressedBytesCount);
                                totalRawBytes += rawBytes;
                                if (!wroteMidToConsole)
                                {
                                    Console.WriteLine();
                                    Console.WriteLine("Large Mid: " + subject);
                                    wroteMidToConsole = true;
                                }
                            }

                            // SY: for the first line only
                            if (lastSubject == "") lastSubject = subject;
                        }
                        else // SY: Output the data of this subject group
                        {
                            // SY: compress and save the unsaved string builder content first, unless multipleCompressedLengths.Count > 0 && builder.Length == 0
                            if (!multipleCompressedLengths.Any() || builder.Length > 0)
                            {
                                long rawBytes;
                                var bytesCount = CompressAndSave(builder, datapagesWriter, out rawBytes);
                                totalRawBytes += rawBytes;
                                multipleCompressedLengths.Add(bytesCount);
                            }

                            // SY: Total size of the compressed data
                            var compressedBytesCount = multipleCompressedLengths.Select(e => (long)e).Sum();

                            // SY: Initialize the dictionary for the next subject group
                            var newKey = GetSubjectKey(subject);
                            if (!midToOffsetDictionaries.ContainsKey(newKey))
                                midToOffsetDictionaries.Add(newKey, new Dictionary<string, Tuple<long, int>>());

                            #region Save the previous block

                            var key = GetSubjectKey(lastSubject);

                            // SY: Add the position and length of the group of "lastSubject" in the offset dictionary 
                            if (midToOffsetDictionaries[key].ContainsKey(lastSubject))
                                throw new Exception("Duplicate runs for mid " + lastSubject + ", line: " + line);
                            midToOffsetDictionaries[key].Add(lastSubject, new Tuple<long, int>(currentOffset, (int)Math.Min(compressedBytesCount, int.MaxValue)));

                            if (isCVT)
                            {
                                if (!cvtNodes.ContainsKey(key))
                                    cvtNodes.Add(key, new Dictionary<string, bool>());
                                if (!cvtNodes[key].ContainsKey(lastSubject))
                                    cvtNodes[key][lastSubject] = true;
                            }

                            if (multipleCompressedLengths.Count > 1)
                            {
                                largeMidsToCompressedBlobsLocations.Add(lastSubject, new Tuple<long, List<int>>(currentOffset, multipleCompressedLengths.ToArray().ToList()));
                            }

                            multipleCompressedLengths.Clear();

                            #endregion

                            // reset "lastSubject" to the current subject and other variables
                            lastSubject = subject;
                            currentOffset += compressedBytesCount;
                            isCVT = true;
                            wroteMidToConsole = false;

                            // don't forget to process the current line (with the new subject), now that the builder has been cleared and isCVT has been reset
                            builder.Append(parts[1]);
                            builder.Append("\t");
                            builder.Append(parts[2]);
                            builder.AppendLine();

                            if (parts[1] == "type.object.name")
                            {
                                isCVT = false;
                                // SY: if an entity has more than one name, this table will only store the last one.
                                namesTable[parts[0]] = parts[2];
                            }

                            #region Update status information for print out

                            avgUncompressedSize = (avgUncompressedSize*currentMidCounts + totalRawBytes)/(currentMidCounts + 1.0);
                            avgCompressedSize = (avgCompressedSize*currentMidCounts + compressedBytesCount)/(currentMidCounts + 1.0);
                            totalRawBytes = 0;
                            currentMidCounts += 1.0;

                            if (((long)currentMidCounts)%10000 == 0)
                                Console.Write(".");

                            if (((long)currentMidCounts)%1000000 == 0)
                            {
                                Console.WriteLine();
                                Console.WriteLine("" + curLines + " lines, " + ((double)currentMidCounts)/1000000.0 + "Million mids, " + avgCompressedSize + " compAvg, " + avgUncompressedSize + " uncompAvg" +
                                                  ", size read = " + avgUncompressedSize*currentMidCounts/(1024*1024*1024) + " GB");
                                GC.Collect();
                            }

                            #endregion
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        break;
                    }
                }

                datapagesWriter.Close();
            }

            using (var dictionaryStream = File.OpenWrite(midToOffsetFilename))
            {
                Serialize(midToOffsetDictionaries, dictionaryStream);
                dictionaryStream.Close();
            }

            using (var largeDictionaryStream = File.OpenWrite(largeMidToOffsetFilename))
            {
                SerializeSimple(largeMidsToCompressedBlobsLocations, largeDictionaryStream);
                largeDictionaryStream.Close();
            }

            using (var cvtNodesStream = File.OpenWrite(cvtNodesFilename))
            {
                SerializeCVTNodes(cvtNodes, cvtNodesStream);
                cvtNodesStream.Close();
            }

            using (var namesTableStream = File.OpenWrite(namesTableFilename))
            {
                SerializeRelationTable(namesTable, namesTableStream);
                namesTableStream.Close();
            }
        }


        public static int CompressAndSave(StringBuilder builder, BinaryWriter datapagesWriter, out long rawBytes)
        {
            var dataAsString = builder.ToString();
            builder.Clear();
            byte[] rawbytes = new byte[dataAsString.Length*sizeof (char)];
            Buffer.BlockCopy(dataAsString.ToCharArray(), 0, rawbytes, 0, rawbytes.Length);
            byte[] compressedBytes;

            using (var memorystream = new MemoryStream())
            {
                var gzipstream = new GZipStream(memorystream, CompressionMode.Compress, false);
                gzipstream.Write(rawbytes, 0, rawbytes.Length);
                gzipstream.Close();
                compressedBytes = memorystream.ToArray();
                memorystream.Close();
            }

            datapagesWriter.Write(compressedBytes);
            rawBytes = rawbytes.Length;
            return compressedBytes.Length;
        }

        public static string GetSubjectKey(string subject)
        {
            // if subject starts from "m." or "g.", then use the first 4 characters as key by default
            //                                      otherwise, use the whole subject
            // if subject starts from neither "m." nor "g.", then use the first 2 characters as key by default;
            //                                      if subject has only 1 character, then use it as key directly

            return (subject.StartsWith("m.") || subject.StartsWith("g.")) ?
                ((subject.Length > 3) ?
                    subject.Substring(0, 4) :
                    (subject.Length > 2) ? subject.Substring(0, 3) : subject.Substring(0, 2)) :
                (subject.Length > 1) ? subject.Substring(0, 2) : subject.Substring(0, 1);
        }

        public static void SerializeCVTNodes(Dictionary<string, Dictionary<string, bool>> cvtnodes, Stream stream)
        {
            var writer = new BinaryWriter(stream);

            writer.Write(cvtnodes.Count);
            foreach (var key in cvtnodes.Keys)
            {
                writer.Write(key);
                var dictionary = cvtnodes[key];
                writer.Write(dictionary.Count);
                foreach (var kvp in dictionary)
                {
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value);
                }
            }
            writer.Flush();

        }

        public static void Serialize(Dictionary<string, Dictionary<string, Tuple<long, int>>> dictionaries, Stream stream)
        {
            var writer = new BinaryWriter(stream);

            writer.Write(dictionaries.Count);
            foreach (var key in dictionaries.Keys)
            {
                writer.Write(key);
                var dictionary = dictionaries[key];
                writer.Write(dictionary.Count);
                foreach (var kvp in dictionary)
                {
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value.Item1);
                    writer.Write(kvp.Value.Item2);
                }
            }
            writer.Flush();
        }

        public static void SerializeSimple(Dictionary<string, Tuple<long, List<int>>> dictionary, Stream stream)
        {
            var writer = new BinaryWriter(stream);

            writer.Write(dictionary.Count);
            foreach (var key in dictionary.Keys)
            {
                writer.Write(key);
                var offset = dictionary[key].Item1;
                var list = dictionary[key].Item2;
                writer.Write(offset);
                writer.Write(list.Count());
                foreach (var val in list)
                {
                    writer.Write(val);
                }
            }
            writer.Flush();
        }

        public static void SerializeRelationTable(Dictionary<string, string> dictionary, Stream stream)
        {
            BinaryWriter writer = new BinaryWriter(stream);
            writer.Write(dictionary.Count);
            foreach (KeyValuePair<string, string> pair in dictionary)
            {
                writer.Write(pair.Key);
                writer.Write(pair.Value);
            }
            writer.Flush();
        }

        public static void TrimData(CommandLineArguments cmd)
        {
            var inputFBFile = Path.Combine(cmd.idir, "freebase-rdf-latest");
            var outputEnglishFile = Path.Combine(cmd.odir, "fb_en.txt");
            var outputEnglishNonMFile = Path.Combine(cmd.odir, "fb_en_nonM.txt");
            var outputConsoleFilename = Path.Combine(cmd.odir, "fb_console.txt");


            var totalTriples = 0;

            var moreThan4PartsCount = 0;

            var subjectFirstTwoLettersHistogram = new Dictionary<string, int>();

            var beforePredicateHistogram = new Dictionary<string, int>();

            var objectIdsPrefixHistogram = new Dictionary<string, int>();
            var objectValueCount = 0;
            var objectValueNoLangIdCount = 0;
            var objectValueEnglishLangIdCount = 0;
            var objectValueForeignLangIdCount = 0;

            // predicates to be removed
            HashSet<string> hsRemovedPred = new HashSet<string>(new[]
            {
                "22-rdf-syntax-ns#type", "type.object.key", "rdf-schema#label", "type.object.permission",
                "type.user.usergroup", "type.usergroup.member", "type.user.userid", "type.permission.controls", "user"
            });
            string[] lstRemovedPredPre = {"user.", "wikipedia.", "dataworld."};

            // Store the entity names even when there is no "en" language
            Dictionary<string, string> dtLang2Name = new Dictionary<string, string>();
            string[] langOrder = {"en", "en-US", "en-GB", "en-CA", "en-Dsrt"};
            string lastSubj = "";
            string lang = "";

            try
            {
                using (FileStream originalFileStream = File.OpenRead(inputFBFile))
                {
                    var reader = new StreamReader(originalFileStream);
                    using (var writerDecompressedEnglish = new StreamWriter(outputEnglishFile))
                    {
                        using (var writernonM = new StreamWriter(outputEnglishNonMFile))
                        {
                            using (var writerConsole = new StreamWriter(outputConsoleFilename))
                            {
                                string line;
                                bool blExistEn;
                                while ((line = reader.ReadLine()) != null)
                                {
                                    if (totalTriples%100000 == 0)
                                        Console.Write(".");
                                    if (totalTriples%10000000 == 0)
                                        Console.WriteLine();
                                    totalTriples++;
                                    var parts = line.Split('\t');

                                    if (parts.Count() != 4)
                                    {
                                        moreThan4PartsCount++;
                                        continue;
                                    }

                                    var subjectStart = parts[0].LastIndexOf('/') + 1;
                                    var subjectEnd = parts[0].LastIndexOf('>');
                                    var subject = parts[0].Substring(subjectStart, subjectEnd - subjectStart);
                                    var subjectPrefix = subject.Substring(0, (subject.Length > 1 ? 2 : 1));
                                    if (!subjectFirstTwoLettersHistogram.ContainsKey(subjectPrefix))
                                        subjectFirstTwoLettersHistogram.Add(subjectPrefix, 1);
                                    else
                                        subjectFirstTwoLettersHistogram[subjectPrefix]++;


                                    var predicateStart = parts[1].LastIndexOf('/') + 1;
                                    var predicateEnd = parts[1].LastIndexOf('>');
                                    var predicate = parts[1].Substring(predicateStart, predicateEnd - predicateStart);

                                    // Check if we want to remove this predicate
                                    if (hsRemovedPred.Contains(predicate))
                                        continue;
                                    bool blRemove = false;
                                    foreach (var prefix in lstRemovedPredPre)
                                    {
                                        if (predicate.StartsWith(prefix))
                                        {
                                            blRemove = true;
                                            break;
                                        }
                                    }
                                    if (blRemove)
                                        continue;

                                    var beforePredicate = parts[1].Substring(0, predicateStart);
                                    if (!beforePredicateHistogram.ContainsKey(beforePredicate))
                                        beforePredicateHistogram.Add(beforePredicate, 1);
                                    else
                                        beforePredicateHistogram[beforePredicate]++;

                                    int objectStart, objectEnd;
                                    string objectG;
                                    if (parts[2][0] != '"') // mid or predicate
                                    {
                                        // <http://rdf.freebase.com/ns/film.performance>
                                        // <http://rdf.freebase.com/ns/m.011f92n6>
                                        // <http://www.imdb.com/name/nm0097986/>


                                        objectStart = parts[2].LastIndexOf('/') + 1;
                                        if (parts[2][objectStart] == '>')
                                        {
                                            // this case: // <http://www.imdb.com/name/nm0097986/>
                                            objectStart = parts[2].Substring(0, objectStart - 1).LastIndexOf('/') + 1;
                                        }
                                        objectEnd = parts[2].LastIndexOf('>');
                                        objectG = parts[2].Substring(objectStart, objectEnd - objectStart);

                                        var objectPrefix = objectG.Substring(0, (objectG.Length > 1 ? 2 : 1));
                                        if (!objectIdsPrefixHistogram.ContainsKey(objectPrefix))
                                            objectIdsPrefixHistogram.Add(objectPrefix, 1);
                                        else
                                            objectIdsPrefixHistogram[objectPrefix]++;
                                    }
                                    else // value 
                                    {
                                        // "9"
                                        // "Laurens Maturana"@en
                                        // "Turtlewax (TurtleWax/6920a285ab8b7f7f) 2014-08-04T18:25:09.133-07:00"@en
                                        // "1922-06-12"^^<http://www.w3.org/2001/XMLSchema#date>
                                        var lastQuotationIndex = parts[2].LastIndexOf('"');
                                        objectG = parts[2].Substring(1, lastQuotationIndex - 1);
                                        if (parts[2].Length > lastQuotationIndex + 1 && parts[2][lastQuotationIndex + 1] == '@')
                                        {
                                            lang = parts[2].Substring(lastQuotationIndex + 2);

                                            if (parts[2].Length > lastQuotationIndex + 3 && lang.StartsWith("en"))
                                            {
                                                objectValueEnglishLangIdCount++;
                                            }
                                            else
                                            {
                                                objectValueForeignLangIdCount++;
                                                if (predicate != "type.object.name" || !subject.StartsWith("m."))
                                                    continue; // filter out foreign stuff, except entity names
                                            }
                                        }
                                        else
                                        {
                                            objectValueNoLangIdCount++;
                                        }
                                        objectValueCount++;
                                    }

                                    // Before moving to the next subject, output the name of the entity
                                    if (subject != lastSubj)
                                    {
                                        if (lastSubj != "")
                                        {
                                            // Output the entity names
                                            blExistEn = false;
                                            foreach (var langEn in langOrder)
                                            {
                                                if (dtLang2Name.ContainsKey(langEn))
                                                {
                                                    writerDecompressedEnglish.WriteLine(lastSubj + "\ttype.object.name\t" + dtLang2Name[langEn]);
                                                    blExistEn = true;
                                                    break;
                                                }
                                            }

                                            if (!blExistEn) // write all names
                                            {
                                                foreach (var objG in dtLang2Name.Values.Distinct())
                                                    writerDecompressedEnglish.WriteLine(lastSubj + "\ttype.object.name\t" + objG);
                                            }
                                        }

                                        // reset variables
                                        lastSubj = subject;
                                        dtLang2Name = new Dictionary<string, string>();
                                    }

                                    if (subject.StartsWith("m."))
                                    {
                                        if (predicate == "type.object.name") // Store first and output in the end
                                            dtLang2Name[lang] = objectG;
                                        else
                                            writerDecompressedEnglish.WriteLine(subject + "\t" + predicate + "\t" + objectG);
                                    }
                                    else
                                    {
                                        writernonM.WriteLine(subject + "\t" + predicate + "\t" + objectG);
                                    }
                                }

                                // Final processing of the name of the entity
                                blExistEn = false;
                                foreach (var langEn in langOrder)
                                {
                                    if (dtLang2Name.ContainsKey(langEn))
                                    {
                                        writerDecompressedEnglish.WriteLine(lastSubj + "\ttype.object.name\t" + dtLang2Name[langEn]);
                                        blExistEn = true;
                                        break;
                                    }
                                }
                                if (!blExistEn) // write all names
                                {
                                    foreach (var objG in dtLang2Name.Values.Distinct())
                                        writerDecompressedEnglish.WriteLine(lastSubj + "\ttype.object.name\t" + objG);
                                }


                                writerConsole.WriteLine("Total Triples: " + totalTriples);

                                writerConsole.WriteLine("Num parts != 4: " + moreThan4PartsCount);

                                writerConsole.WriteLine("Subject: ");
                                subjectFirstTwoLettersHistogram.Keys.Select(subj => new {subj, count = subjectFirstTwoLettersHistogram[subj]})
                                    .Where(e => e.count > 1000)
                                    .OrderByDescending(e => e.count).ToList()
                                    .ForEach(e => writerConsole.WriteLine("\t" + e.count + ": " + e.subj));

                                writerConsole.WriteLine("Before Predicate");
                                beforePredicateHistogram.Keys.Select(beforeP => new {beforeP, count = beforePredicateHistogram[beforeP]})
                                    .Where(e => e.count > 1000)
                                    .OrderByDescending(e => e.count).ToList()
                                    .ForEach(e => writerConsole.WriteLine("\t" + e.count + ": " + e.beforeP));

                                writerConsole.WriteLine("Object Ids");
                                objectIdsPrefixHistogram.Keys.Select(obj => new {obj, count = objectIdsPrefixHistogram[obj]})
                                    .Where(e => e.count > 1000)
                                    .OrderByDescending(e => e.count).ToList()
                                    .ForEach(e => writerConsole.WriteLine("\t" + e.count + ": " + e.obj));

                                var objectsWithIdsCount = objectIdsPrefixHistogram.Keys.Select(e => objectIdsPrefixHistogram[e]).Sum();
                                writerConsole.WriteLine("Objects With Ids:      " + objectsWithIdsCount);
                                writerConsole.WriteLine("Objects With Values:   " + objectValueCount);
                                writerConsole.WriteLine("   No Lang Id:            " + objectValueNoLangIdCount);
                                writerConsole.WriteLine("   English:               " + objectValueEnglishLangIdCount);
                                writerConsole.WriteLine("   Foreign:               " + objectValueForeignLangIdCount);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + Environment.NewLine + (e.InnerException ?? new Exception("")).Message);
            }
        }

        public static void FindGhost(CommandLineArguments cmd)
        {
            var fnNameTable = Path.Combine(cmd.idir, "namesTable.bin");
            var fnTupleFile1 = Path.Combine(cmd.idir, "fb_en.txt");
            var fnTupleFile2 = Path.Combine(cmd.idir, "fb_en_nonM.txt");
            var fnGhost = Path.Combine(cmd.odir, "ghost_mid.txt");

            HashSet<string> setObj = new HashSet<string>(), setSub = new HashSet<string>();
            long lnCnt = 0;
            var namesTable = DeserializeRelationTable(File.OpenRead(fnNameTable));

            var startTime = DateTime.Now;

            foreach (var ln in File.ReadLines(fnTupleFile1).Concat(File.ReadLines(fnTupleFile2)))
            {
                if (++lnCnt % 1000000 == 0)
                {
                    var retrieveSec = (DateTime.Now - startTime).TotalSeconds;
                    Console.Error.WriteLine("[{0:0.00}] Processed {1}M lines.", retrieveSec, lnCnt / 1000000);
                }
                var f = ln.Split('\t');
                string sub = f[0], obj = f[2];

                if ((obj.StartsWith("m.") || obj.StartsWith("g.")) && // not a value node
                    (!namesTable.ContainsKey(obj))) // not an entity node, a candidate ghost object
                    setObj.Add(obj);

                if (!namesTable.ContainsKey(sub)) // not an entity node
                    setSub.Add(sub);
            }

            var fGhost = new StreamWriter(fnGhost);
            foreach (var x in setObj.Except(setSub))
            {
                fGhost.WriteLine("{0}", x);
            }
            fGhost.Close();
        }

        private static Dictionary<string, string> DeserializeRelationTable(Stream stream)
        {
            BinaryReader reader = new BinaryReader(stream);
            int dictionaryCount = reader.ReadInt32();
            Dictionary<string, string> relationDictionary = new Dictionary<string, string>(dictionaryCount);
            for (int i = 0; i < dictionaryCount; i++)
            {
                string key = reader.ReadString();
                string value = reader.ReadString();
                relationDictionary[key] = value;
            }
            return relationDictionary;
        }
    }
}