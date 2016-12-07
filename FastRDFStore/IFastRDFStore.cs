using System.Runtime.Serialization;
using System.ServiceModel;

namespace FastRDFStore
{
    [ServiceContract(Namespace = "urn:ps")]
    public interface IFastRDFStore
    {
        [OperationContract]
        string[] GetOutboundPredicates(string subjectMid);

        [OperationContract]
        string[] GetEntityNames(string[] entMids);

        [OperationContract]
        SimpleFBObject GetSimpleObjectPredicatesAndCVTs(string subjectMid, int maxPerPredicate, bool followCVT);

        [OperationContract]
        SimpleFBObject GetSimpleObjectFilteredPredicateAndObjects(string subjectMid, string predicate);

        [OperationContract]
        string[][] FindNodeSquencesOnPredicateChain(string startMid, string[] chainPredicates);
    }
}