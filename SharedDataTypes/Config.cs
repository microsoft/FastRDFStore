using System.Collections.Generic;
using System.IO;

namespace CSI
{
    static public class Config
    {
        public const string dirWork = @"\\tspace10\e$\users\tmsnwork";
        public static string dirDat = Path.Combine(dirWork, "Data");
        public static string fnStopWords = Path.Combine(dirDat, "short-stopwords.txt");
        public static HashSet<string> setMaleKW = new HashSet<string>(new string[] { "dad", "father", "brother", "grandfather", "grandson", "son", "husband" });
        public static HashSet<string> setFemaleKW = new HashSet<string>(new string[] { "mom", "mother", "sister", "grandmother", "granddaughter", "daughter", "wife" });
        public static HashSet<string> setTimeKW = new HashSet<string>(new string[] { "when", "time", "year", "date", "old", "birthdate", "birthday" });
        public const int MaxEntityCandidates = 12;

        public const double Epsilon = 1e-10;
    }
}