using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ServiceModel;
using CommandLine;
using FastRDFStore;


/// <summary>
/// A command line client to call FastRDFStore WCF service.
/// </summary>

namespace FastRDFStoreClient
{
    internal class CommandLineArguments
    {
        // Connect to the RDFStore server at this location/port
        [Option('s', "server", HelpText = "Connect to the FastRDFStore server on this server [localhost]", DefaultValue = "localhost")]
        public string server { get; set; }

        [Option('p', "port", HelpText = "Connect to the FastRDFStore server on this port [9358]", DefaultValue = 9358)]
        public int port { get; set; }

        [Option('d', "dump", HelpText = "DumpMID")]
        public bool dump { get; set; }

        [Option('m', "mid", HelpText = "MID to search for")]
        public string mid { get; set; }

        [Option('t', "tripleOnly", HelpText = "Triple Only")]
        public bool tripleOnly { get; set; }

        [Option("pred", HelpText = "(optional) predicate for filtering")]
        public string predicate { get; set; }

        [Option('c', "chain", HelpText = "Predicate chain to search for")]
        public string predicateChain { get; set; }

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
        private static void Main(string[] args)
        {
            CommandLineArguments cmd = new CommandLineArguments();
            Parser.Default.ParseArgumentsStrict(args, cmd);

            var binding = new NetTcpBinding(SecurityMode.None);
            binding.MaxBufferSize = int.MaxValue;
            binding.MaxBufferPoolSize = int.MaxValue;
            binding.MaxReceivedMessageSize = int.MaxValue;

            // Don't need identity because we're connecting without security. If we need security later, uncomment the following lines
            //EndpointIdentity identity = EndpointIdentity.CreateUpnIdentity(System.Security.Principal.WindowsIdentity.GetCurrent().Name);
            var myEndpoint = new EndpointAddress(new Uri("net.tcp://" + cmd.server + ":" + cmd.port + "/solver") /*, identity*/);
            var myChannelFactory = new ChannelFactory<IFastRDFStore>(binding, myEndpoint);
            IFastRDFStore fastRDFStoreClient = myChannelFactory.CreateChannel();

            Console.WriteLine("Endpoint connected to " + myEndpoint.Uri);
            do
            {
                if (cmd.mid != null && cmd.predicateChain != null)
                {
                    var result = fastRDFStoreClient.FindNodeSquencesOnPredicateChain(cmd.mid, cmd.predicateChain.Split(' '));
                    if (result == null)
                        Console.WriteLine("Nothing is found.");
                    else
                    {
                        foreach(var seq in result)
                            Console.WriteLine(string.Join("\t", seq));
                    }
                    return;
                }

                if (cmd.mid != null && cmd.tripleOnly)
                {
                    var result = fastRDFStoreClient.GetSimpleObjectPredicatesAndCVTs(cmd.mid, int.MaxValue, false);

                    //var result = fastRDFStoreClient.GetPredObj(cmd.mid);
                    if (result == null)
                        Console.WriteLine("Nothing is found.");
                    else
                    {
                        foreach (var po in result.Objects)
                        {
                            foreach (var node in po.Objects)
                            {
                                string type, val;
                                if (node is ValueFBObject)
                                {
                                    type = "Literal";
                                    val = node.GetNameOrValue();
                                }
                                else if (node is CVTFBObject)
                                {
                                    type = "CVT";
                                    val = node.GetMid();
                                }
                                else // (node is SimpleFBObject)
                                {
                                    type = "Entity";
                                    val = node.GetMid();
                                }

                                Console.WriteLine("{0}\t{1}\t{2}", po.Predicate, type, val);
                            }
                        }
                    }
                    return;
                }

                string subject = cmd.mid;
                string predicate = cmd.predicate;

                if (subject == null)
                {
                    Console.WriteLine("Enter a Mid (m.06w2sn5) or hit enter to also enter a predicate.");
                    Console.Write("Enter subject: ");
                    subject = Console.ReadLine();
                }

                if (subject == "")
                {
                    Console.WriteLine("First enter a Mid (m.06w2sn5) then a predicate (people.person.parents).");
                    // Example predicate to try
                    // m.06w2sn5 (Justin Bieber)
                    // people.person.parents (a non CVT relationship)
                    // people.person.sibling_s people.sibling_relationship.sibling (a CVT mediated relationship)
                    // Or:
                    // m.019nnl (Family Guy)
                    // tv.tv_program.regular_cast tv.regular_tv_appearance.actor

                    Console.Write("Enter subject: ");
                    subject = Console.ReadLine();
                    Console.Write("Enter predicate: ");
                    predicate = Console.ReadLine();
                }

                var startTime = DateTime.Now;
                SimpleFBObject results;
                double retrieveSec;

                if (predicate == null)
                    results = fastRDFStoreClient.GetSimpleObjectPredicatesAndCVTs(subject, int.MaxValue, true);
                else
                    results = fastRDFStoreClient.GetSimpleObjectFilteredPredicateAndObjects(subject, predicate);
                retrieveSec = (DateTime.Now - startTime).TotalSeconds;

                HashSet<string> alreadyOutput = new HashSet<string>();
                alreadyOutput.Add(results.Mid);
                foreach (PredicateAndObjects predAndObjs in results.Objects)
                {
                    if (cmd.dump)
                        OutputMids(predAndObjs, alreadyOutput);
                    else
                        OutputPredicateAndObjects(predAndObjs, alreadyOutput);
                }

                Console.WriteLine("Took " + retrieveSec + " seconds to retrieve results ");
                Console.WriteLine();
            } while (cmd.mid == null); // Loop forever if using console input
        }

        private static void OutputMids(PredicateAndObjects predAndObjects, HashSet<string> alreadyOutput = null)
        {
            if (alreadyOutput == null)
                alreadyOutput = new HashSet<string>();
            foreach (FBObject fbObj in predAndObjects.Objects)
            {
                if (fbObj is SimpleFBObject)
                {
                    SimpleFBObject simpleFBObj = fbObj as SimpleFBObject;
                    Console.WriteLine(simpleFBObj.Mid);
                }
                else if (fbObj is CVTFBObject)
                {
                    CVTFBObject cvtObj = fbObj as CVTFBObject;
                    Console.WriteLine(cvtObj.Mid);
                    if (!alreadyOutput.Contains(cvtObj.Mid))
                    {
                        alreadyOutput.Add(cvtObj.Mid);
                        foreach (PredicateAndObjects cvtObject in cvtObj.Objects)
                            OutputMids(cvtObject, alreadyOutput);
                    }
                }
            }
        }

        private static void OutputPredicateAndObjects(PredicateAndObjects predAndObjects, HashSet<string> alreadyOutput = null, int indent = 0)
        {
            if (alreadyOutput == null)
                alreadyOutput = new HashSet<string>();

            if (indent != 0)
                Console.Write(new string(' ', indent));
            Console.Write(string.Format("{0,-40} --> ", predAndObjects.Predicate));

            bool newlineWritten = false;
            foreach (FBObject fbObj in predAndObjects.Objects)
            {
                if (newlineWritten)
                    Console.Write(string.Format("{0,-40} --> ", ""));
                if (fbObj is ValueFBObject)
                {
                    Console.WriteLine((fbObj as ValueFBObject).Value);
                }
                else if (fbObj is SimpleFBObject)
                {
                    SimpleFBObject simpleFBObj = fbObj as SimpleFBObject;
                    Console.WriteLine((simpleFBObj.Name ?? "[no name]") + " (" + simpleFBObj.Mid + ")");
                    if (simpleFBObj.Objects != null)
                    {
                        if (!alreadyOutput.Contains(simpleFBObj.Mid))
                        {
                            alreadyOutput.Add(simpleFBObj.Mid);
                            foreach (PredicateAndObjects Object in simpleFBObj.Objects)
                                OutputPredicateAndObjects(Object, alreadyOutput, indent + 4);
                        }
                    }
                }
                else if (fbObj is CVTFBObject)
                {
                    CVTFBObject cvtObj = fbObj as CVTFBObject;
                    Console.WriteLine("CVT (" + cvtObj.Mid + ")");
                    if (!alreadyOutput.Contains(cvtObj.Mid))
                    {
                        alreadyOutput.Add(cvtObj.Mid);
                        foreach (PredicateAndObjects cvtObject in cvtObj.Objects)
                            OutputPredicateAndObjects(cvtObject, alreadyOutput, indent + 4);
                    }
                }
                else
                {
                    Console.WriteLine("[Unknown object: " + fbObj);
                }
                newlineWritten = true;
            }
        }
    }
}