using System;
using System.Runtime.Serialization;

namespace FastRDFStore
{

    public enum FBNodeType
    {
        Value,
        Entity,
        CVT
    };

    [DataContract(IsReference = true)]
    public class PredicateAndObjects
    {
        [DataMember]
        public string Predicate { get; set; }

        [DataMember]
        public FBObject[] Objects { get; set; }
    }

    // A FBObject can either be:
    //   - a simple value ("42"):   ValueFBObject(value="42")
    //   - an entity (Ireland):     SimpleFBObject(mid="m.012wgb", name="Ireland")
    //   - a CVT node (dated integer value)  CVTFBObject
    [DataContract(IsReference = true)]
    [KnownType(typeof (ValueFBObject))]
    [KnownType(typeof (SimpleFBObject))]
    [KnownType(typeof (CVTFBObject))]
    public abstract class FBObject
    {
        public abstract string PrettyString();
        public abstract string GetNameOrValue();

        public virtual string GetMid() { return String.Empty; }
    }

    [DataContract(IsReference = true)]
    public class ValueFBObject : FBObject
    {
        [DataMember]
        public string Value { get; set; }

        public override string PrettyString() { return Value; }
        public override string GetNameOrValue() { return Value; }
    }

    [DataContract(IsReference = true)]
    public class SimpleFBObject : FBObject
    {
        [DataMember]
        public string Mid { get; set; }

        [DataMember]
        public string Name { get; set; }

        [DataMember]
        public PredicateAndObjects[] Objects { get; set; }

        // Guaranteed that each predicate appears only once. May be null

        public override string PrettyString() { return Name; }
        public override string GetNameOrValue() { return Name; }
        public override string GetMid() { return Mid; }
    }

    [DataContract(IsReference = true)]
    public class CVTFBObject : FBObject
    {
        [DataMember]
        public string Mid { get; set; } // mattri: Is this needed? If not used, could remove to save network traffic

        [DataMember]
        public PredicateAndObjects[] Objects { get; set; }

        // Guaranteed that each predicate appears only once. mattri: Can a CVT node have the same predicate coming off of it twice, with different objects? If not, replace with just an array of <predicate,object> pairs

        public override string PrettyString() { return "[CVT " + Mid + "]"; }
        public override string GetNameOrValue() { return ""; }
        public override string GetMid() { return Mid; }
    }


}