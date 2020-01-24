/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.walking;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.NodeWalker;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class NodeWalkerReconstructingProc  {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(name = "algo.randomWalkReconstructing.stream", mode = Mode.READ)
    @Description("CALL algo.randomWalkReconstructing.stream(start:null=all/[ids]/label, steps, walks, {graph: 'heavy/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH'}) " +
            "YIELD nodes, path - computes random walks from given starting points")
    public Stream<WalkResult> randomWalkReconstructing(
            @Name(value = "start", defaultValue = "null") Object start,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        String label = configuration.getNodeLabelOrQuery();
        String relationship = configuration.getRelationshipOrQuery();

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }

        Number returnParam = configuration.get("return", 1d);
        Number inOut = configuration.get("inOut", 1d);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        Boolean returnPath = configuration.get("path", false);

        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);

        PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator();

        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
        return randomWalks
                .map( nodes -> new WalkResult(nodes, returnPath ? WalkPath.toPathReconstructing(api, nodes, direction) : null));
    }
   
    
    @Procedure(name = "algo.randomWalkReconstructingString.stream", mode = Mode.READ)
    @Description("CALL algo.randomWalkReconstructingString.stream(start:null=all/[ids]/label, steps, walks, outputFilename, labelURIMap, {graph: 'heavy/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH', labelURIMap:}) ")             
    public Stream<RDF2VecOutput> randomWalkReconstructingString(
            @Name(value = "start", defaultValue = "null") Object start,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "labelURIMap", defaultValue="{}") Map<String, String> labelURIMap, 
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
    	
    	config.put("path", true); 
    	
    	ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        String label = configuration.getNodeLabelOrQuery();
        String relationship = configuration.getRelationshipOrQuery();

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }

        Number returnParam = configuration.get("return", 1d);
        Number inOut = configuration.get("inOut", 1d);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        Boolean returnPath = configuration.get("path", false);

        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);

        PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator();

        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
        return randomWalks
                .map( nodes -> WalkPath.toPathReconstructingRDF2Vec(api, nodes, direction, labelURIMap));
    }
    
    @Procedure(name = "algo.randomWalkReconstructingStringSaving", mode = Mode.READ)
    @Description("CALL algo.randomWalkReconstructingStringSaving(start:null=all/[ids]/label, steps, walks, outputFilename, labelURIMap, {graph: 'heavy/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH', labelURIMap:}) ")             
    public void randomWalkReconstructingStringSaving(
            @Name(value = "start", defaultValue = "null") Object start,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "labelURIMap", defaultValue="{}") Map<String, String> labelURIMap, 
            @Name(value = "outputFilename", defaultValue= "null") String outputFilename, 
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
    	
    	if (outputFilename == null) return; 
    	
    	config.put("path", true); 
    	
    	ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        String label = configuration.getNodeLabelOrQuery();
        String relationship = configuration.getRelationshipOrQuery();

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return;
        }

        Number returnParam = configuration.get("return", 1d);
        Number inOut = configuration.get("inOut", 1d);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        Boolean returnPath = configuration.get("path", false);

        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);

        PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator();

        while (idStream.hasNext()) {
			System.out.print(idStream.next() + " ");
		}
		System.out.println(); 
        
        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
        
        try {
	        Files.write(Paths.get(outputFilename), 
	        		(Iterable<String>) randomWalks.parallel().map(nodes -> WalkPath.toPathReconstructingRDF2Vec(api, nodes, direction, labelURIMap))
					.map(x -> x.toString())::iterator, 
					StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
        	System.err.println("Problems writing the results"); 
        	e.printStackTrace();
        }
        
    }
    
    @Procedure(name = "algo.randomWalkReconstructingStringSavingLabel", mode = Mode.READ)
    @Description("CALL algo.randomWalkReconstructingStringSavingLabel(start:null=all/[ids]/label, steps, walks, outputFilename, labelURIMap, {graph: 'heavy/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH', labelURIMap:}) ")             
    public void randomWalkReconstructingStringSavingLabel(
            @Name(value = "label", defaultValue = "null") String label,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "labelURIMap", defaultValue="{}") Map<String, String> labelURIMap, 
            @Name(value = "outputFilename", defaultValue= "null") String outputFilename, 
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
    	
    	if (outputFilename == null) return; 
    	
    	config.put("path", true); 
    	
    	ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        // this: checks whether the query or a label is given 
        // Query in the config map 
        // label-> it checks the start parameter, if it's not found as label, it goes for everything
        // in this method we force an all parameter via loading the graph with null param. 
        
//        String label = configuration.getNodeLabelOrQuery();
        String relationship = configuration.getRelationshipOrQuery();
        
        // we force to load all the graph
        final Graph graph = load("all", relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return;
        }

        Number returnParam = configuration.get("return", 1d);
        Number inOut = configuration.get("inOut", 1d);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        int concurrency = configuration.getConcurrency();
        Boolean returnPath = configuration.get("path", false);
        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);
        
        // we use it as a semaphore
        Object lock = new Object(); 
        
        InternalTransaction trans = api.beginTransaction(Type.explicit, null); 
        Stream<Node> targetStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(api.findNodes(Label.label(label)), Spliterator.ORDERED),
                false); 
        
        targetStream.parallel().forEach(
        	start -> {
        		System.out.println(start.getLabels() + " " + start.getProperties("iri")+ " "+start.getId()); 
        		// IdStream is a little bit tricky as it depends on the type of object you pass, 
        		// in this case, we will need to pass the ID of each one
        		PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start.getId(), graph, limit)).limit(limit).iterator();
        		// we just synchronize locally the access to the result vector
                Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
                Vector<String> walkStrings = new Vector<> ();  
                randomWalks.parallel().map(nodes -> WalkPath.toPathReconstructingRDF2Vec(api, nodes, direction, labelURIMap)).map(x -> walkStrings.add(x.toString())); 
                // the access to the file must be globally synchronized
                synchronized (lock) {
                	try {
            	        Files.write(Paths.get(outputFilename), 
            	        		walkStrings, 
            					StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }
                    catch (IOException e) {
                    	System.err.println("Problems writing the results"); 
                    	e.printStackTrace();
                    }
				}
        	}
        );
        trans.close();
    }

    
    
//    @Procedure(name = "algo.randomWalkReconstructingSaving", mode = Mode.READ)
//    @Description("CALL algo.randomWalkReconstructingSaving(start:null=all/[ids]/label, steps, walks, outputFilename, {graph: 'heavy/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH', labelURIMap:}) ")             
//    public void randomWalkReconstructingSaving(
//            @Name(value = "start", defaultValue = "null") Object start,
//            @Name(value = "steps", defaultValue = "10") long steps,
//            @Name(value = "walks", defaultValue = "1") long walks,
//            @Name(value = "labelURIMap", defaultValue="{}") Map<String, String> labelURIMap, 
//            @Name(value = "outputFilename", defaultValue = "null") String outputFilename, 
//            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//    	if (outputFilename == null) return; 
//
//    	
//    	
//    	java.nio.file.Path path = Paths.get(outputFilename); 
//    	config.put("path", true); 
//    	try {
//    		Files.write(path, randomWalkReconstructing(start, steps, walks, config).map(result -> WalkPath.toStringRDF2vec(result))::iterator, StandardOpenOption.CREATE_NEW );
//    	}
//    	catch (Exception e){
//    		e.printStackTrace();
//    	}
//    	
//    	ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
//
//        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
//
//        AllocationTracker tracker = AllocationTracker.create();
//
//        Direction direction = configuration.getDirection(Direction.BOTH);
//
//        String label = configuration.getNodeLabelOrQuery();
//        String relationship = configuration.getRelationshipOrQuery();
//
//        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);
//
//        int nodeCount = Math.toIntExact(graph.nodeCount());
//
//        if(nodeCount == 0) {
//            graph.release();
//            return Stream.empty();
//        }
//
//        Number returnParam = configuration.get("return", 1d);
//        Number inOut = configuration.get("inOut", 1d);
//        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
//                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
//                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());
//
//        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
//
//        int concurrency = configuration.getConcurrency();
//
//        Boolean returnPath = configuration.get("path", false);
//
//        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);
//
//        PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator();
//
//        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
//        return randomWalks
//                .map( nodes -> new WalkResult(nodes, returnPath ? WalkPath.toPathReconstructing(api, nodes, direction) : null));
//    	
//    	
//    	
//    	
//    	
//    	
//    }

    private IntStream idStream(@Name(value = "start", defaultValue = "null") Object start, Graph graph, int limit) {
        int nodeCount = Math.toIntExact(graph.nodeCount());
        System.out.println(start.getClass().getName()); 
        if (start instanceof String) {
            String label = start.toString();
            int labelId = transaction.tokenRead().nodeLabel(label);
            int countWithLabel = Math.toIntExact(transaction.dataRead().countsForNodeWithoutTxState(labelId));
            NodeLabelIndexCursor cursor = transaction.cursors().allocateNodeLabelIndexCursor();
            transaction.dataRead().nodeLabelScan(labelId, cursor);
            cursor.next();
            LongStream ids;
            if (limit == -1) {
                ids = LongStream.range(0, countWithLabel).map( i -> cursor.next() ? cursor.nodeReference() : -1L );
            } else {
                int[] indexes = ThreadLocalRandom.current().ints(limit + 1, 0, countWithLabel).sorted().toArray();
                IntStream deltas = IntStream.range(0, limit).map(i -> indexes[i + 1] - indexes[i]);
                ids = deltas.mapToLong(delta -> { while (delta > 0 && cursor.next()) delta--;return cursor.nodeReference(); });
            }
            return ids.mapToInt(graph::toMappedNodeId).onClose(cursor::close);
        } else if (start instanceof Collection) {
            return ((Collection)start).stream().mapToLong(e -> ((Number)e).longValue()).mapToInt(graph::toMappedNodeId);
        } else if (start instanceof Number) {
            return LongStream.of(((Number)start).longValue()).mapToInt(graph::toMappedNodeId);
        } else {
            if (nodeCount < limit) {
                return IntStream.range(0,nodeCount).limit(limit);
            } else {
                return IntStream.generate(() -> ThreadLocalRandom.current().nextInt(nodeCount)).limit(limit);
            }
        }
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder, ProcedureConfiguration configuration) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withDirection(configuration.getDirection(Direction.BOTH))
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutRelationshipWeights();


        try (ProgressTimer timer = ProgressTimer.start()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

}
