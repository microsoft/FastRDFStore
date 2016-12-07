using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace FastRDFStore
{
    public class FastRDFStore : IFastRDFStore
    {
        private const int pageSize = 1024*1024*1024; // 1GB

        private static Object datalock = new Object();
        private static bool initialized;
        private static Dictionary<string, Dictionary<string, Tuple<long, int>>> midToCompressedBlobLocation;
        private static List<byte[]> datapages;
        private static Dictionary<string, Tuple<long, List<int>>> largeMidsToCompressedBlobsLocations;
        private static Dictionary<string, Dictionary<string, bool>> cvtNodes;
        private static Dictionary<string, string> namesTable;
        private static Dictionary<string, FBNodeType> predObjTypeTable;
        private static HashSet<string> setGhostMid;
        private static Logger logger;

        private static string datadir;

        public static void Initialize(string datadirParam, string logFilename)
        {
            datadir = datadirParam;
            
            logger = new Logger(logFilename);
            logger.Log("Initializing FastRDFStore");

            try
            {
                if (!initialized)
                {
                    lock (datalock)
                    {
                        if (!initialized)
                            // avoid race condition with another thread also trying to initialize at the same time
                        {
                            LoadIndex();
                            initialized = true;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Log it, but rethrow so the caller knows the initialization failed.
                logger.LogException("Exception when initializing FastRDFStore", e);
                throw;
            }

            logger.Log("Done initializing");
        }

        public string[] GetOutboundPredicates(string subjectMid)
        {
            try
            {
                logger.Log("GetOutboundPredicates called for " + subjectMid);
                if (!initialized || subjectMid == null || subjectMid.Length < 2) return new string[] {};

                var results = GetPredicateObjectPairsForSubject(subjectMid);

                var adjoiningPredicates = results.AsParallel().Where(e => !e.Item1.StartsWith("wikipedia.") &&
                                                                          e.Item1 != "type.object.type" &&
                                                                          e.Item1 != "type.object.key" &&
                                                                          e.Item1 != "type.object.name" &&
                                                                          e.Item1 != "type.object.permission" &&
                                                                          e.Item1 != "common.topic.alias" &&
                                                                          e.Item1 != "common.topic.description" &&
                                                                          e.Item1 != "common.topic.image" &&
                                                                          !IsCVT(e.Item2)).Select(e => e.Item1);
                var adjPredCnt = adjoiningPredicates.GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count());


                var cvtHopResults = results.AsParallel().Where(e => IsCVT(e.Item2) &&
                                                                    !e.Item1.StartsWith("wikipedia.") &&
                                                                    e.Item1 != "common.topic.article" &&
                                                                    e.Item1 != "common.topic.webpage" &&
                                                                    e.Item1 != "common.topic.description" &&
                                                                    e.Item1 != "common.document.text" &&
                                                                    e.Item1 != "common.topic.image");
                var cvtPredCnt = cvtHopResults.GroupBy(x => x.Item1).ToDictionary(g => g.Key, g => g.Count());

                // Check if there is any conflict and try to resolve it
                var conflictPreds = new HashSet<string>(adjPredCnt.Keys.Intersect(cvtPredCnt.Keys));
                foreach(var cPred in conflictPreds)
                {
                    if (adjPredCnt[cPred] > cvtPredCnt[cPred])
                        cvtPredCnt.Remove(cPred);
                    else if (adjPredCnt[cPred] < cvtPredCnt[cPred])
                        adjPredCnt.Remove(cPred);
                    logger.Log("Cannot resolve adj-cvt predicate conflict: " + subjectMid + " " + cPred);
                }

                var cvtHopPredicates = cvtHopResults.Where(e => cvtPredCnt.ContainsKey(e.Item1))
                                                    .Select(e => new
                                                            {
                                                                predicate = e.Item1,
                                                                cvt = e.Item2,
                                                                cvtPredicates = GetPredicateObjectPairsForSubject(e.Item2).Select(pair => pair.Item1)
                                                                    .Where(predicate2 => predicate2 != "type.object.type" &&
                                                                                         predicate2 != "type.object.key" &&
                                                                                         predicate2 != "common.topic.description" &&
                                                                                         predicate2 != "common.document.text").Distinct()
                                                            })
                                                            .SelectMany(e => e.cvtPredicates.Select(predicate2 => e.predicate + " " + predicate2));

                var allPredicates = adjoiningPredicates.Where(x => adjPredCnt.ContainsKey(x)).Union(cvtHopPredicates).OrderBy(e => e);

                return allPredicates.ToArray();
            }
            catch (Exception e)
            {
                logger.LogException("GetOutboundPredicates failed", e);
                return new string[] { };
            }
        }

        public string[] GetEntityNames(string[] entMids)
        {
            try
            {
                return entMids.Select(mid => namesTable.ContainsKey(mid) ? namesTable[mid] : "")
                              .ToArray();
            }
            catch (Exception e)
            {
                logger.LogException("GetEntityNames failed", e);
                return new string[] { };
            }
        }

        // public method which doesn't return the dictionary of nodes in the graph.
        public SimpleFBObject GetSimpleObjectPredicatesAndCVTs(string subjectMid, int maxPerPredicate = int.MaxValue, bool followCVT = true)
        {
            try
            {
                logger.Log("GetSimpleObjectPredicatesAndCVTs called for "+subjectMid);
                Dictionary<string, FBObject> nodesInGraph;
                return GetSimpleObjectPredicatesAndCVTs(subjectMid, out nodesInGraph, maxPerPredicate, followCVT);
            }
            catch (Exception e)
            {
                logger.LogException("GetSimpleObjectPredicatesAndCVTs failed", e);
                return null;
            }
        }

        private SimpleFBObject GetSimpleObjectPredicatesAndCVTs(string subjectMid,
            out Dictionary<string, FBObject> nodesInGraph, int maxPerPredicate = int.MaxValue, bool followCVT = true)
        {
            SimpleFBObject myself = new SimpleFBObject();
            myself.Mid = subjectMid;
            myself.Name = GetName(subjectMid);
            Dictionary<string, FBObject> existingNodes = new Dictionary<string, FBObject>();
            existingNodes[subjectMid] = myself;
            myself.Objects = GetPredicatesAndNamedObjectsIncludingCVTs(existingNodes, subjectMid, maxPerPredicate,
                followCVT);
            nodesInGraph = existingNodes;
            return myself;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="existingNodes"></param>
        /// <param name="subjectMid"></param>
        /// <param name="maxPerPredicate"></param>
        /// <param name="followCVT"></param>
        /// <returns>Predicates and objects hanging off of subjectMid. We guarantee that each predicate appears only once in the array</returns>
        private PredicateAndObjects[] GetPredicatesAndNamedObjectsIncludingCVTs(
            Dictionary<string, FBObject> existingNodes, string subjectMid, int maxPerPredicate = int.MaxValue,
            bool followCVT = true)
        {
            List<Tuple<string, string>> results = GetPredicateObjectPairsForSubject(subjectMid);
            Dictionary<string, List<FBObject>> predicatesToReturn = new Dictionary<string, List<FBObject>>();
            Dictionary<string, int> predicateCountDict = new Dictionary<string, int>();
            foreach (Tuple<string, string> pair in results)
            {
                string predicate = pair.Item1;
                string obj = pair.Item2;

                // Check if the obj type is legit
                FBNodeType legitObjType, objType;
                objType = IsCVT(obj) ? FBNodeType.CVT : (IsEntity(obj) ? FBNodeType.Entity : FBNodeType.Value);
                if (predObjTypeTable.TryGetValue(predicate, out legitObjType) && objType != legitObjType)
                    continue;

                // Check if obj is a ghost MID using the pre-compiled ghost MID table
                if (objType != FBNodeType.Value && setGhostMid.Contains(obj))
                    continue;

                // Check if obj is a ghost MID if (1) it's not in the cvtNodes and (2) it does not have an entity name
                // This may happen because we do not index some tuples because of the predicates are excluded.
                if (IsEntity(obj) && !IsCVT(obj) && string.IsNullOrEmpty(GetName(obj)))
                    continue;

                // Skip this predicate if we have added it maxPerPredicate times
                int predicateCount;
                predicateCountDict.TryGetValue(predicate, out predicateCount);
                // sets predicateCount to 0 if not in the dictionary
                if (predicateCount >= maxPerPredicate)
                    continue; // Skip any more predicates, we've reached our max
                predicateCountDict[predicate] = predicateCount + 1;

                // Get the list of answers we're returning for this predicate
                List<FBObject> predicateObjects;
                if (!predicatesToReturn.TryGetValue(predicate, out predicateObjects))
                {
                    predicateObjects = new List<FBObject>();
                    predicatesToReturn[predicate] = predicateObjects;
                }

                if (objType == FBNodeType.Entity)
                {
                    FBObject fbObject;
                    if (!existingNodes.TryGetValue(obj, out fbObject))
                    {
                        SimpleFBObject simpleFBObject = new SimpleFBObject();
                        simpleFBObject.Mid = obj;
                        simpleFBObject.Name = GetName(obj);
                        existingNodes[obj] = simpleFBObject;
                        fbObject = simpleFBObject;
                    }
                    predicateObjects.Add(fbObject);
                }
                else if (objType == FBNodeType.Value)
                {
                    ValueFBObject fbObject = new ValueFBObject();
                    fbObject.Value = obj;
                    predicateObjects.Add(fbObject);
                }
                else if (followCVT) // (objType == FBNodeType.CVT)
                {
                    FBObject fbObject;
                    if (!existingNodes.TryGetValue(obj, out fbObject))
                    {
                        CVTFBObject cvtFBObject = new CVTFBObject();
                        cvtFBObject.Mid = obj;
                        cvtFBObject.Objects = GetPredicatesAndNamedObjectsIncludingCVTs(existingNodes, obj,
                            maxPerPredicate, false /* don't follow CVT nodes from this CVT node */);
                        existingNodes[obj] = cvtFBObject;
                        fbObject = cvtFBObject;
                    }
                    predicateObjects.Add(fbObject);
                }
            }

            // Convert to the return type (arrays instead of lists and dictionaries)
            return
                predicatesToReturn.Select(
                    pair => new PredicateAndObjects() {Predicate = pair.Key, Objects = pair.Value.ToArray()}).ToArray();
        }

        private void FilterToSinglePredicate(SimpleFBObject node, string predicate)
        {
            PredicateAndObjects predicateAndObject = null;
            foreach (PredicateAndObjects p in node.Objects)
            {
                if (p.Predicate == predicate)
                {
                    predicateAndObject = p;
                    break;
                }
            }
            if (predicateAndObject == null)
            {
                // Didn't find the predicate 
                node.Objects = new PredicateAndObjects[0];
            }
            node.Objects = new PredicateAndObjects[1] {predicateAndObject};
        }


        public SimpleFBObject GetSimpleObjectFilteredPredicateAndObjects(string subjectMid, string predicate)
        {
            try
            {
                logger.Log("GetSimpleObjectFilteredPredicateAndObjects called for subj=" + subjectMid + ", pred="+predicate);

                Dictionary<string, FBObject> nodesInGraph;
                SimpleFBObject initial = GetSimpleObjectPredicatesAndCVTs(subjectMid, out nodesInGraph, int.MaxValue, true);

                string[] predicateParts = predicate.Split(' ');

                if (predicateParts.Length < 1 || predicateParts.Length > 2) return null;

                FilterToSinglePredicate(initial, predicateParts[0]);
                if (initial.Objects.Length == 0)
                    return initial; // Doesn't contain the desired predicate
                PredicateAndObjects predicateAndObjects = initial.Objects[0];

                if (predicateParts.Length == 2)
                {
                    foreach (FBObject fbo in predicateAndObjects.Objects)
                    {
                        if (fbo is CVTFBObject)
                        {
                            foreach (PredicateAndObjects poi in (((CVTFBObject)fbo).Objects))
                            {
                                if (poi.Predicate == predicateParts[1])
                                {
                                    foreach (FBObject fboObj in poi.Objects)
                                    {
                                        if (fboObj is SimpleFBObject)
                                        {
                                            SimpleFBObject fboAnswer = (SimpleFBObject)fboObj;
                                            if (fboAnswer.Objects == null)
                                            {
                                                // We need to expand the objects for this node
                                                PredicateAndObjects[] resultsForObj =
                                                    GetPredicatesAndNamedObjectsIncludingCVTs(nodesInGraph, fboAnswer.Mid,
                                                        int.MaxValue, false);
                                                fboAnswer.Objects = resultsForObj;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
                else
                {
                    foreach (FBObject fbo in predicateAndObjects.Objects)
                    {
                        if (fbo is SimpleFBObject)
                        {
                            SimpleFBObject fboAnswer = (SimpleFBObject)fbo;
                            if (fboAnswer.Objects == null)
                            {
                                // We need to expand the objects for this node
                                PredicateAndObjects[] resultsForObj = GetPredicatesAndNamedObjectsIncludingCVTs(
                                    nodesInGraph, fboAnswer.Mid, int.MaxValue, false);
                                fboAnswer.Objects = resultsForObj;
                            }
                        }
                    }

                }
                return initial;
            }
            catch (Exception e)
            {
                logger.LogException("GetSimpleObjectFilteredPredicateAndObjects failed", e);
                return null;
            }
        }

        public string[][] FindNodeSquencesOnPredicateChain(string startMid, string[] chainPredicates)
        {
            try
            {
                //logger.Log("FindNodeSquencesOnPredicateChain called for subj=" + startMid + ", chainPreds=" + string.Join(" ", chainPredicates));

                if (chainPredicates == null || chainPredicates.Length == 0)
                    return null;

                var pred = chainPredicates[0];  // first predicate on the chain
                var objNodes = GetPredicateObjectPairsForSubject(startMid)
                    .Where(x => x.Item1 == pred) // (predicate, object)
                    .Select(x => x.Item2) // object only
                    .ToArray();
                if (!objNodes.Any())
                    return null;

                if (chainPredicates.Length == 1) // done
                    return objNodes.Select(x => new[] {x}).ToArray();

                // more than one predicate in the chain
                var ret = new List<string[]>();
                foreach (var node in objNodes)
                {
                    var subSequences = FindNodeSquencesOnPredicateChain(node, chainPredicates.Skip(1).ToArray());
                    if (subSequences == null) // cannot continue
                        continue;
                    ret.AddRange(subSequences.Select(seq => (new[] {node}).Concat(seq).ToArray()));
                }
                return ret.Any() ? ret.ToArray() : null;
            }
            catch (Exception e)
            {
                logger.LogException("FindNodeSquencesOnPredicateChain failed", e);
                return null;
            }
        }

        private string GetName(string mid)
        {
            if (mid == null)
                return null;

            string name;
            if (namesTable.TryGetValue(mid, out name))
                return name;
            else
                return null;
        }

        private bool IsCVT(string subject)
        {
            var key = GetSubjectKey(subject);
            if (cvtNodes.ContainsKey(key))
            {
                var dictionary = cvtNodes[key];
                return dictionary.ContainsKey(subject);
            }
            return false;
        }

        private bool IsEntity(string obj)
        {
            // We're missing this information in the compressed dataset. 
            // For now, we'll do the following, but long-term consider fixing this to make it explicit in the dataset
            return obj.StartsWith("m.") || obj.StartsWith("g.") || 
                   obj.StartsWith("en.") || !string.IsNullOrEmpty(GetName(obj));   // "en." is to support SEMPRE Freebase
        }

        private List<Tuple<string, string>> GetPredicateObjectPairsForSubject(string subject)
        {
            try
            {
                long offset;
                var compressedChunksLengths = new List<int>();
                if (largeMidsToCompressedBlobsLocations.ContainsKey(subject))
                {
                    var found = largeMidsToCompressedBlobsLocations[subject];
                    offset = found.Item1;
                    compressedChunksLengths.AddRange(found.Item2);
                }
                else
                {
                    var partitionkey = GetSubjectKey(subject);
                    var dictionary = midToCompressedBlobLocation[partitionkey];
                    var compressedResultLocation = dictionary[subject];
                    offset = compressedResultLocation.Item1;
                    var length = compressedResultLocation.Item2;
                    compressedChunksLengths.Add(length);
                }

                var toReturn = new List<Tuple<string, string>>();
                foreach (var length in compressedChunksLengths)
                {
                    // does it span pages?
                    var startPage = (int)(offset/pageSize);
                    var endPage = (int)((offset + length - 1)/pageSize);
                    byte[] compressedResult;
                    int compressedResultIndex;
                    int compressedResultCount;
                    if (startPage == endPage)
                    {
                        compressedResult = datapages[(int)(offset/pageSize)];
                        compressedResultIndex = (int)(offset%pageSize);
                        compressedResultCount = length;
                    }
                    else
                    {
                        compressedResult = new byte[length];
                        compressedResultIndex = 0;
                        compressedResultCount = length;
                        // first page
                        int index = 0;
                        for (int i = (int)(offset%pageSize); i < pageSize; i++)
                        {
                            compressedResult[index] = datapages[startPage][i];
                            index++;
                        }

                        // intermediary pages
                        for (int page = startPage + 1; page < endPage; page++)
                        {
                            for (int i = 0; i < pageSize; i++)
                            {
                                compressedResult[index] = datapages[page][i];
                                index++;
                            }
                        }

                        // last page
                        for (int i = 0; i < (int)((offset + length)%pageSize); i++)
                        {
                            compressedResult[index] = datapages[endPage][i];
                            index++;
                        }
                    }

                    using (
                        var memorystream = new MemoryStream(compressedResult, compressedResultIndex,
                            compressedResultCount))
                    {
                        var gzipstream = new GZipStream(memorystream, CompressionMode.Decompress, false);
                        var reader = new StreamReader(gzipstream, Encoding.Unicode);
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            var split = line.Split('\t');
                            if (split.Length == 2 && !string.IsNullOrEmpty(split[0]))
                                toReturn.Add(new Tuple<string, string>(split[0], split[1]));
                        }
                    }

                    offset += length;
                }

                return toReturn;
            }
            catch (Exception e)
            {
                logger.LogException("GetPredicateObjectPairsForSubject failed", e);
                return new List<Tuple<string, string>>();
            }
        }


        private static string GetSubjectKey(string subject)
        {
            return (subject.StartsWith("m.") || subject.StartsWith("g."))
                ? ((subject.Length > 3)
                    ? subject.Substring(0, 4)
                    : (subject.Length > 2) ? subject.Substring(0, 3) : subject.Substring(0, 2))
                : (subject.Length > 1)
                    ? subject.Substring(0, 2)
                    : subject.Substring(0, 1);
        }

        private static void LoadIndex()
        {
            string midToOffsetPath = Path.Combine(datadir, "midToOffset.bin");
            string largeMidToOffsetPath = Path.Combine(datadir, "largeMidToOffset.bin");
            string datapagesPath = Path.Combine(datadir, "datapages.bin");
            string cvtNodesPath = Path.Combine(datadir, "cvtnodes.bin");
            string namesTablePath = Path.Combine(datadir, "namesTable.bin");
            string predicateObjTypePath = Path.Combine(datadir, "predicate.objtype.txt");
            string ghostMidPath = Path.Combine(datadir, "ghost_mid.txt");

            logger.Log("Reading the ghost MID table");
            setGhostMid = new HashSet<string>(File.ReadAllLines(ghostMidPath));

            logger.Log("Reading the Predicate Objective Type table");
            predObjTypeTable = new Dictionary<string, FBNodeType>();
            foreach (var x in File.ReadLines(predicateObjTypePath)
                .Select(ln => ln.Split('\t'))
                .Select(
                    f =>
                        new
                        {
                            pred = f[0],
                            valcnt = long.Parse(f[1]),
                            entcnt = long.Parse(f[2]),
                            cvtcnt = long.Parse(f[3])
                        }))
            {
                if ((x.valcnt == 0 && x.entcnt == 0) ||
                    (x.entcnt == 0 && x.cvtcnt == 0) ||
                    (x.valcnt == 0 && x.cvtcnt == 0)) // no inconsistency in the data, skip
                    continue;

                if (x.valcnt >= Math.Max(x.entcnt, x.cvtcnt))
                    predObjTypeTable.Add(x.pred, FBNodeType.Value);
                else if (x.entcnt >= Math.Max(x.valcnt, x.cvtcnt))
                    predObjTypeTable.Add(x.pred, FBNodeType.Entity);
                else
                    predObjTypeTable.Add(x.pred, FBNodeType.CVT);
            }

            logger.Log("Reading names table");
            namesTable = DeserializeRelationTable(File.OpenRead(namesTablePath));
            logger.Log("Reading index");
            midToCompressedBlobLocation = Deserialize(File.OpenRead(midToOffsetPath));
            largeMidsToCompressedBlobsLocations = DeserializeSimple(File.OpenRead(largeMidToOffsetPath));
            cvtNodes = DeserializeCVTNodes(File.OpenRead(cvtNodesPath));
            datapages = new List<byte[]>();

            using (var binreader = new BinaryReader(File.OpenRead(datapagesPath)))
            {
                while (true)
                {
                    var page = binreader.ReadBytes(pageSize);
                    datapages.Add(page);
                    if (page.Length < pageSize)
                        break;
                }
            }
        }


        private static Dictionary<string, Dictionary<string, Tuple<long, int>>> Deserialize(Stream stream)
        {
            var reader = new BinaryReader(stream);

            var dictionariesCount = reader.ReadInt32();
            var toReturn = new Dictionary<string, Dictionary<string, Tuple<long, int>>>();
            for (int i = 0; i < dictionariesCount; i++)
            {
                var key = reader.ReadString();
                int count = reader.ReadInt32();
                var dictionary = new Dictionary<string, Tuple<long, int>>(count);
                for (int n = 0; n < count; n++)
                {
                    var subject = reader.ReadString();
                    var offset = reader.ReadInt64();
                    var bytecount = reader.ReadInt32();
                    dictionary.Add(subject, new Tuple<long, int>(offset, bytecount));
                }
                toReturn.Add(key, dictionary);
            }
            return toReturn;
        }

        private static Dictionary<string, Dictionary<string, bool>> DeserializeCVTNodes(Stream stream)
        {
            var reader = new BinaryReader(stream);

            var dictionariesCount = reader.ReadInt32();
            var toReturn = new Dictionary<string, Dictionary<string, bool>>();
            for (int i = 0; i < dictionariesCount; i++)
            {
                var key = reader.ReadString();
                int count = reader.ReadInt32();
                var dictionary = new Dictionary<string, bool>(count);
                for (int n = 0; n < count; n++)
                {
                    var mid = reader.ReadString();
                    var isCVT = reader.ReadBoolean();
                    dictionary.Add(mid, isCVT);
                }
                toReturn.Add(key, dictionary);
            }
            return toReturn;
        }

        private static Dictionary<string, Tuple<long, List<int>>> DeserializeSimple(Stream stream)
        {
            var reader = new BinaryReader(stream);

            var dictionaryCount = reader.ReadInt32();
            var toReturn = new Dictionary<string, Tuple<long, List<int>>>();
            for (int n = 0; n < dictionaryCount; n++)
            {
                var subject = reader.ReadString();
                var offset = reader.ReadInt64();
                var numCounts = reader.ReadInt32();
                var list = new List<int>();
                for (int i = 0; i < numCounts; i++)
                    list.Add(reader.ReadInt32());
                toReturn.Add(subject, new Tuple<long, List<int>>(offset, list));
            }
            return toReturn;
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