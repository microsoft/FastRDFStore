using System;
using System.IO;
using System.Text;

namespace FastRDFStore
{
    public class Logger
    {
        public enum Severity { INFO, WARNING, ERROR, EXCEPTION };

        public object outputLock = new object();
        private readonly string logFilename;

        public Logger(string logFilename) { this.logFilename = logFilename; }

        private static string EscapeNewlineAndTab(string s)
        {
            return s.Replace("\r\n", "\\n").Replace("\n\r", "\\n").Replace("\r", "\\n").Replace("\n", "\\n").Replace("\t", "\\t");
        }

        public void LogException(string message, Exception e)
        {
            string fullMessage = message + ". Exception info: " + e.ToString();
            if (e.InnerException != null)
                fullMessage += " *** With InnerException: " + e.InnerException.ToString();
            Log(fullMessage, Severity.EXCEPTION);
        }

        public void Log(string message, Severity severity = Severity.INFO)
        {
            if (string.IsNullOrWhiteSpace(logFilename))  // empty log file name -> skip logging
                return;

            lock (outputLock)
            {
                // Use "sortable" datetime for later log file processing convenience
                string line = DateTime.Now.ToString("s") + "\t" + severity + "\t" + EscapeNewlineAndTab(message);
                Console.WriteLine(line);
                File.AppendAllText(logFilename, line + Environment.NewLine);
            }
        }

    }
}