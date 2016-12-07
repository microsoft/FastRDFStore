using System;
using System.ServiceModel;
using System.Threading;
using CommandLine;

// This is the project that starts the FastRDFStore WCF service.

namespace FastRDFStore
{
    internal class CommandLineArguments
    {
        [Option('i', "idir", HelpText = "Directory containing *.bin files", DefaultValue = "")]
        public string idir { get; set; }

        [Option('s', "server", HelpText = "Server", DefaultValue = "localhost")]
        public string server { get; set; }

        [Option('p', "port", HelpText = "Connect to the FastRDFStore server on this port", DefaultValue = 9358)]
        public int port { get; set; }

        [Option('l', "log", HelpText = "Log file. Set to empty to disable logging", DefaultValue = "FastRDFStore.log")]
        public string logfile { get; set; }

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

            FastRDFStore.Initialize(cmd.idir, cmd.logfile);

            StartRDFStoreService(cmd.server, cmd.port);

            // Wait for user to hit CTRL-C
            Thread.Sleep(Timeout.Infinite);
        }

        public static void StartRDFStoreService(string server, int port)
        {
            var sh = new ServiceHost(typeof (FastRDFStore));

            var binding = new NetTcpBinding(SecurityMode.None)
            {
                MaxBufferSize = int.MaxValue,
                MaxBufferPoolSize = int.MaxValue,
                MaxReceivedMessageSize = int.MaxValue,
                ReceiveTimeout = TimeSpan.MaxValue,
                CloseTimeout = TimeSpan.MaxValue,
                TransferMode = TransferMode.Buffered
            };

            binding.ReaderQuotas.MaxDepth = int.MaxValue;
            //binding.MaxConnections = 5;
            //binding.ListenBacklog = 5;

            var endPointStringSolver = String.Format("net.tcp://{0}:{1}/solver", server, port);
            sh.AddServiceEndpoint(typeof (IFastRDFStore), binding, endPointStringSolver);

            sh.Open();

        }
    }
}