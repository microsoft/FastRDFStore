#MSR FastRDFStore Package
-----

## Overview

The MSR FastRDFStore Package is designed for creating an in-memory index of RDF triples, implemented as a WCF service in C#, and consists of server & client side code. RDF triples are the standard format for storing structured knowledge graphs. Instead of relying on a complete SPARQL server engine to index and serve the data from RDF triples, our software package provides the essential functions for traversing the knowledge graph in a much more efficient way.

In addition to the binary executables and the source code, the package includes the last dump of Freebase ([freebase-rdf-2015-08-09-00-01.gz](https://developers.google.com/freebase/)), as well as the processed version ready to load directly into FastRDFStore. Users who would like to use the package for Freebase do not need to compile the package and process the raw data, but instead can run the executables directly. The executables can be directly run on Windows, or on Linux using [Mono](http://www.mono-project.com/ "Cross platform, open source .NET framework").

FastRDFStore was originally designed to support the creation of the [WebQuestions Semantic Parses Dataset (WebQSP)](https://www.aka.ms/WebQSP "WebQuestions Semantic Parses Dataset"). Details on this dataset can be found at our ACL-2016 paper: Yih, Richardson, Meek, Chang & Suh. "[The Value of Semantic Parse Labeling for Knowledge Base Question Answering](https://aclweb.org/anthology/P/P16/P16-2033.pdf)."

## Run FastRDFStore on Freebase

If you just need to run the FastRDFStore WCF server on the Freebase data provided in this package, simply use the following command to start the FastRDFStore server.

  * ```bin\FastRDFStore.exe -i data```

Notice that running the FastRDFStore service to serve this Freebase data will need about 50GB memory. Initializing the server takes about 14 minutes. Once the service starts, you can use the command line client tool to test it.

  * ```bin\FastRDFStoreClient.exe```

By typing an entity id in Freebase(i.e., MID), it will output the triples where the given MID is the subject. When the object is a CVT node, it will output triples with the CVT node as the subject as well. Below is an example:

```
Enter subject: m.0c5g7w5
common.topic.notable_for                 --> CVT (g.1yg9b9lpq)
    common.notable_for.predicate             --> /type/object/type
    common.notable_for.display_name          --> Musical Track
                                         --> Musical Recording
    common.notable_for.object                --> Musical Recording (m.0kpv11)
    common.notable_for.notable_object        --> Musical Recording (m.0kpv11)
base.schemastaging.topic_extra.review_webpage --> Round_%2526_Round_(Selena_Gomez_%2526_the_Scene_song)
music.recording.contributions            --> CVT (m.0ccbt6k)
    music.track_contribution.track           --> Round & Round (m.0c5g7w5)
    music.track_contribution.contributor     --> Selena Gomez (m.0gs6vr)
common.topic.notable_types               --> Musical Recording (m.0kpv11)
music.recording.producer                 --> Kevin Rudolf (m.03f5drm)
music.recording.length                   --> 308.0
common.topic.webpage                     --> CVT (m.0ccbrdk)
    common.webpage.resource                  --> Wikipedia (m.0ccbrdf)
    common.webpage.category                  --> Review (m.09rg1d4)
    common.webpage.topic                     --> Round & Round (m.0c5g7w5)
kg.object_profile.prominent_type         --> Musical Track (music.recording)
common.topic.article                     --> CVT (m.0ccbrm8)
    common.document.updated                  --> 2010-07-08T20:12:00.330017Z
    common.document.text                     --> \"Round & Round\" is a song by American band Selena Gomez & the Scene. The song was written by Selena Gomez, Fefe Dobson, and Cash Money's Kevin Rudolf, who also produced the song. The song is an electronica-based dance-pop song with rock and disco beats. It was released as the lead single from the band's sophomore album, A Year Without Rain on June 22, 2010.
    common.document.content                  --> type.object.name                         --> Round & Round
Took 0.071488 seconds to retrieve results
```

## Details of Projects

Below, we provide more detailed descriptions of the projects, data and other folders included in this package.

### FastRDFStore

This is the RDFStore WCF service we provided. Available command line arguments are:
```
bin\FastRDFStore.exe -h
FastRDFStore.exe Usage:


  -i, --idir      (Default: ) Directory containing *.bin files
  -s, --server    (Default: localhost) Server [localhost]
  -p, --port      (Default: 9358) Connect to the FastRDFStore server on this port
  -l, --log       (Default: FastRDFStore.log) Log file. Set to empty to disable logging
  --help          Display this help screen.
```

Functions supported in this service are defined in the interface file ```IFastRDFStore.cs```:

* ```string[] GetOutboundPredicates(string subjectMid);```
  
  Return all the predicates starting with *subjectMid*. If any predicate leads to a CVT node, then all outbound predicates from the CVT node are also followed. These predicates are represented by a space-delimited string "predicate1 predicate2", where predicate1 leads from the subjectMid to the CVT node, and predicate2 is a predicate off of the CVT node.

* ```string[] GetEntityNames(string[] entMids);```

  Return the names of given entity ids (*entMids*). Names are determined using the "type.object.name" relation for the entity. 
  
* ```SimpleFBObject GetSimpleObjectPredicatesAndCVTs(string subjectMid, int maxPerPredicate, bool followCVT);```

  Returns a graph of predicates and objects reachable from the given subject. The SimpleFBObject contains all of predicates of which subjectMid is a subject, and for each predicate contains a list of all objects reachable by following the predicate from the given subject. It will also follow CVT nodes for one hop, if requested. That is, if a predicate points to a CVT node, then all outgoing predicates from that node (and all corresponding objects) will also be returned. Note that the same object may be reachable through more than one predicate, and is deduplicated in the returned graph.
  
* ```SimpleFBObject GetSimpleObjectFilteredPredicateAndObjects(string subjectMid, string predicate);```

  Similar to GetSimpleObjectPredicatesAndCVTs, this returns a graph containing predicates and objects reachable from the given subjectMid. In this case, it is filtered to objects reachable via the given predicate. The predicate may be a space-delimited string containing two predicates in order to walk through a CVT node. For example, "music.recording.contributions music.track_contribution.track". 

* ```string[][] FindNodeSquencesOnPredicateChain(string startMid, string[] chainPredicates);```

  Return the lists of intermediate nodes connected by the given chain of predicates (*chainPredicates*) starting from the node *startMid*

-----
  
### FastRDFStoreClient

This command-line client tool is useful for querying the FastRDFStore service in either batch or interactive mode. Available command line arguments are:

```
bin\FastRDFStoreClient.exe -h
FastRDFStoreClient.exe Usage:


  -s, --server        (Default: localhost) Connect to the FastRDFStore server on this server [localhost]
  -p, --port          (Default: 9358) Connect to the FastRDFStore server on this port [9358]
  -d, --dump          DumpMID
  -m, --mid           MID to search for
  -t, --tripleOnly    Triple Only
  --pred              (optional) predicate for filtering
  -c, --chain         Predicate chain to search for
  --help              Display this help screen.
```

When the MID is given, the code is in batch mode and dumps the results to standard output.  This is useful when using a script to run FastRDFStore. Arguments --tripleOnly and --chain are only valid in batch mode; the former outputs only the triples with MID as the subject (without expanding the CVT triples) and the latter only outputs nodes on a given predicate chain.

-----

### FastToRDFStore

This is the utility to process the raw Freebase dump into binary and text data files used by FastRDFStore.  For instance, taking the Freebase dump *freebase-rdf-2015-08-09-00-01.gz* as the original input file, we need to run the following commands to generate the data files.

```zcat freebase-rdf-2015-08-09-00-01.gz > data/freebase-rdf-latest```

```
# Preserve only the Freebase triples needed
bin\FreebaseToRDFStore.exe -c TrimData -i data -o data
```

```
# Build the compressed, binary RDF store files
bin\FreebaseToRDFStore.exe -c BuildStore -i data -o data
```

```
# Find ghost entity nodes that no subject nodes can link to
bin\FreebaseToRDFStore.exe -c BuildStore -i data -o data
```

Once you have run this sequence of commands, you can run the FastRDFStore server on the data directory, as outlined above.

-----

### Notes on compiling using Mono

When using Mono to compile FastRDFStore, the package CommandLineParser.1.9.71 needs to be installed first via NuGet. 
```
$ wget http://nuget.org/nuget.exe -P bin
$ mono bin/nuget.exe install FastRDFStore/packages.config -OutputDirectory packages
```
After that, you can then directly run *xbuild*.
```
$ xbuild FastRDFStore.sln
```
