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
import java.util.concurrent.ConcurrentHashMap;

public class TriangleCountImpl {
    private byte[] input;
    private int numCores;

    public TriangleCountImpl(byte[] input, int numCores) {
		this.input = input;
		this.numCores = numCores;
    }

    public List<String> getGroupMembers() {
		ArrayList<String> ret = new ArrayList<String>();
		ret.add("prli");
		ret.add("l22fu");
		ret.add("p8zhao");
		return ret;
    }
	
	private class TriangleCountRunnable implements Runnable {

		int threadId;
		List adjacencyList;
		List ret;
		public TriangleCountRunnable(int threadId, ArrayList<ArrayList<Integer>> adjacencyList, ArrayList<Triangle> ret) {
			this.threadId = threadId;
			this.adjacencyList = Collections.synchronizedList(adjacencyList);
			this.ret = Collections.synchronizedList(ret);
		}
		
		public void run() {
			int numVertices = adjacencyList.size();
			System.out.println("numVertices:" +  numVertices + " ...");
			for (int i = threadId; i < numVertices; i += numCores) {
				System.out.println("Thread " +  threadId + " running...");
				ArrayList<Integer> n1 = (ArrayList<Integer>)adjacencyList.get(i);
				ConcurrentHashMap<Integer,Integer> hs1 = new ConcurrentHashMap<Integer,Integer> (n1.size());
				System.out.println("node size:" +  n1.size() + " ...");
				for(int j:n1)
				{
					hs1.putIfAbsent(j,1);
					// ArrayList<Integer> temp = (ArrayList<Integer>) adjacencyList.get(j);
					// temp.remove(new Integer(i));
				}
				
				for (int j : n1) {
					
					if (i > j) {
						continue;
					}
					ArrayList<Integer> n2 = (ArrayList<Integer>)adjacencyList.get(j);

					for (int l: n2) {
						if (j > l) {
							continue;
						}
						if (hs1.containsKey(l)) {    
							ret.add(new Triangle(i, j, l));
							System.out.println("Thread " +  threadId + " adding...");
						}
					}
				}
			}
		}
	}

    public List<Triangle> enumerateTriangles() throws IOException {
	
	    ArrayList<ArrayList<Integer>> adjacencyList = getAdjacencyList(input);
    	ArrayList<Triangle> ret = new ArrayList<Triangle>();
	
		for(int i = 0; i < numCores; i++)
		{
			Thread t = new Thread(new TriangleCountRunnable(i, adjacencyList, ret));
			t.start();
		}

    	return ret;
    }

    public ArrayList<ArrayList<Integer>> getAdjacencyList(byte[] data) throws IOException {
		InputStream istream = new ByteArrayInputStream(data);
		BufferedReader br = new BufferedReader(new InputStreamReader(istream));
		String strLine = br.readLine();
		if (!strLine.contains("vertices") || !strLine.contains("edges")) {
			System.err.println("Invalid graph file format. Offending line: " + strLine);
			System.exit(-1);	    
		}
		String parts[] = strLine.split(", ");
		int numVertices = Integer.parseInt(parts[0].split(" ")[0]);
		int numEdges = Integer.parseInt(parts[1].split(" ")[0]);
		System.out.println("Found graph with " + numVertices + " vertices and " + numEdges + " edges");
	 
		ArrayList<ArrayList<Integer>> adjacencyList = new ArrayList<ArrayList<Integer>>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			adjacencyList.add(new ArrayList<Integer>());
		}
		while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
			parts = strLine.split(": ");
			int vertex = Integer.parseInt(parts[0]);
			if (parts.length > 1) {
			parts = parts[1].split(" +");
			for (String part: parts) {
				int neighbour = Integer.parseInt(part);
				if (neighbour > vertex) {
					adjacencyList.get(vertex).add(neighbour);
				}
			}
			}
		}
		br.close();
		return adjacencyList;
    }
}
