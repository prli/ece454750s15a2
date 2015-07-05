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
import java.util.concurrent.ConcurrentLinkedQueue;

public class TriangleCountImpl {
	private byte[] data;
	private int numCores;
	private int numNodes;
	private int numEdges;
	private HashSet<Integer>[] adjacencyList;
	private ConcurrentLinkedQueue<Integer> threadList;
	private ConcurrentHashMap<Triangle, Integer> triangles;
	
	public TriangleCountImpl(byte[] data, int numCores) throws IOException {
		this.data = data;
		this.numCores = numCores;
		threadList = new ConcurrentLinkedQueue<Integer>();
		constructList();
		triangles = new ConcurrentHashMap<Triangle, Integer>();
	}

	public List<String> getGroupMembers() {
		return Arrays.asList("prli", "l22fu", "p8zhao");
	}

	public List<Triangle> enumerateTriangles() throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(numCores);
		
		for (Integer node : threadList) {
			executor.submit(new TriangleCountRunnable(node));
		}
		
        executor.shutdown();
		
		try {
            executor.awaitTermination(120,TimeUnit.SECONDS);
        } 
		catch (InterruptedException e) {
            System.out.println("executor interrupted!");
            System.exit(-1);
        }

        System.out.println("Number of triangles found: " + triangles.size());
				
		return new ArrayList<Triangle>(triangles.keySet());
	}
	
	private class TriangleCountRunnable implements Runnable {

		int threadId;
		public TriangleCountRunnable(int threadId) {
			this.threadId = threadId;
		}

		public void run() {
			HashSet<Integer> Xn = adjacencyList[threadId];
			for (Integer y: Xn) {
				for (Integer z: adjacencyList[y]) {
					if (Xn.contains(z)) {
						triangles.put(new Triangle(threadId, y, z), 0);
					}
				}
			}
		}
	}

	public void constructList() throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(numCores);

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

		for (int i = 0; i < numNodes; i++) {
			
			strLine = br.readLine();
			executor.submit(new ConstructListRunnable(strLine));
		}
	
		br.close();
        executor.shutdown();
		
		try {
            executor.awaitTermination(120,TimeUnit.SECONDS);
        } 
		catch (InterruptedException e) {
            System.out.println("executor interrupted!");
            System.exit(-1);
        }

        //System.out.println("Number of triangles found: " + triangles.size());
	}
	
	@SuppressWarnings("unchecked")
	private class ConstructListRunnable implements Runnable {

		String line;
		public ConstructListRunnable( String l) {
			this.line = l;
		}

		public void run() {		
		
			String parts[] = line.split(": ");
			int node = Integer.parseInt(parts[0]);
			if (parts.length > 1) {
				parts = parts[1].split(" +");
				for (String part: parts) {
					int neighbour = Integer.parseInt(part);
					if (neighbour > node) {
						adjacencyList[node].add(neighbour);
					}
				}
				if(adjacencyList[node].size() > 0) {
					threadList.add(node);
				}
			}
		
		}
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
