/**
 * ECE 454/750: Distributed Computing
 *
 * Code written by Wojciech Golab, University of Waterloo, 2015
 *
 * IMPLEMENT YOUR SOLUTION IN THIS FILE
 *
 */

package ece454750s15a2;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TriangleCountImpl {
	private byte[] data;
	private int numCores;
	private int numNodes;
	private int numEdges;
	private HashSet<Integer>[] adjacencyList;
	private ConcurrentHashMap<Triangle, Triangle> triangles;
	
	public TriangleCountImpl(byte[] data, int numCores) throws IOException {
		this.data = data;
		this.numCores = numCores;
		printMemory();
		adjacencyList = constructList();
		triangles = new ConcurrentHashMap<Triangle, Triangle>();
	}

	public List<String> getGroupMembers() {
		return Arrays.asList("prli", "l22fu", "p8zhao");
	}

	public List<Triangle> enumerateTriangles() throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(numCores);
		int nodeCounter = 0;
		for (int x = 0; x < numNodes; x++) {
            executor.submit(new TriangleCountRunnable(x));
        }
		
        executor.shutdown();
		
		try {
            executor.awaitTermination(60,TimeUnit.SECONDS);
        } 
		catch (InterruptedException e) {
            System.out.println("executor interrupted!");
            System.exit(1);
        }

        System.out.println("Number of triangles found: " + triangles.size());
				
		return new ArrayList<Triangle>(triangles.values());
	}
	
	private class TriangleCountRunnable implements Runnable {

		int threadId;
		public TriangleCountRunnable(int threadId) {
			this.threadId = threadId;
		}

		public void run() {
			HashSet<Integer> Xn = adjacencyList[threadId];
			for (Integer y: Xn) {

				if (threadId > y) {
					return;
				}
				
				HashSet<Integer> Yn = adjacencyList[y];
				for (Integer z: Yn) {
					if (y > z) {
						return;
					}
					if (Xn.contains(z)) {
						triangles.put(new Triangle(threadId, y, z), new Triangle(threadId, y, z));
					}
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private HashSet<Integer>[] constructList() throws IOException {
		
		InputStream istream = new ByteArrayInputStream(data);
		BufferedReader br = new BufferedReader(new InputStreamReader(istream));
		String strLine = br.readLine();
		if (!strLine.contains("vertices") || !strLine.contains("edges")) {
			System.err.println("Invalid graph file format. Offending line: " + strLine);
			System.exit(-1);	    
		}
		String parts[] = strLine.split(", ");
		numNodes = Integer.parseInt(parts[0].split(" ")[0]);
		numEdges = Integer.parseInt(parts[1].split(" ")[0]);
		System.out.println("Nodes " + numNodes);
		System.out.println("Edges " + numEdges);
		
		adjacencyList = new HashSet[numNodes];
		
		for (int i = 0; i < numNodes; i++) {
			adjacencyList[i] = new HashSet<Integer>();
		}
		
		for(int i = 0;i < numNodes; i++) {
			strLine = br.readLine();
			parts = strLine.split(": ");
			int node = Integer.parseInt(parts[0]);
			if (parts.length > 1) {
				parts = parts[1].split(" +");
				for (String part: parts) {
					int neighbour = Integer.parseInt(part);
					if (neighbour > node) {
						adjacencyList[node].add(neighbour);
					}
				}
			}
		}
		
		br.close();
		return adjacencyList;
	}
	
	private void printMemory() {
		int mb = 1024*1024;
        
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
	}
}