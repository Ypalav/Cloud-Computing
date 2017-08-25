import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class ExternalMergeQuickSort extends Thread {
	private static String[] strTempList;

	static String inputfile;
	static String outputfile;
	static int helpersWorking = 1;
	static int numOfThreads;
	static String tempFileLocation;
	static List<File> l;
	static List<String> staticListOfUnsortedFiles;

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("please number of threads");
			return;
		}
		tempFileLocation = "/home/ubuntu/SharedMem";
		int numOfThreads = Integer.parseInt(args[0]);
		try {
			double startTime = System.nanoTime();
			l = divideFilesInChunk(new File("/home/ubuntu/Dataset1"));
			// After dividing the thread run method is called
			Thread[] thread = new Thread[numOfThreads];
			for (int i = 0; i < numOfThreads; i++) {
				thread[i] = new ExternalMergeQuickSort();
				thread[i].start();
			}
			for (int i = 0; i < numOfThreads; i++)
				thread[i].join();
			// Merge function which takes the list of files and passes it to the
			// other merge function for single list mergin
			mergeSortedFiles(l, new File("/home/ubuntu/Output1gb"));
			double endTime = System.nanoTime();
			System.out.println("total time " + ((endTime - startTime) / 1000000000));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Get the file, quicksort it.
	public void run() {
		BufferedReader bufferedReader = null;
		BufferedWriter bufferedWriter = null;
		int left = 0;
		int right = 0;
		String line;
		staticListOfUnsortedFiles = new ArrayList<String>();
		for (int i = 0; i < l.size(); i++) {
			staticListOfUnsortedFiles.clear();
			try {
				bufferedReader = new BufferedReader(new FileReader(l.get(i)));
				while ((line = bufferedReader.readLine()) != null) {
					staticListOfUnsortedFiles.add(line);
				}
				quickSort(0, staticListOfUnsortedFiles.size() - 1);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				bufferedWriter = new BufferedWriter(new FileWriter(l.get(i)));
			} catch (IOException e) {
				e.printStackTrace();
			}
			for (int k = 0; k < staticListOfUnsortedFiles.size(); k++) {
				try {
					bufferedWriter.write(staticListOfUnsortedFiles.get(k));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				try {
					bufferedWriter.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					bufferedWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		try {
			bufferedReader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Get first file and then loop in to other files and pass both files to
	// merge function.
	// The merge function will merge and output the data to first file.
	// So, we have one main file and we are comparing the data and adding the
	// data to the first file
	private static void mergeSortedFiles(List<File> l, File file) throws IOException {

		String line = "";
		List<String> tmplist = new ArrayList<String>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(l.get(0)));
		try {
			while ((line = bufferedReader.readLine()) != null) {
				tmplist.add(line);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			bufferedReader.close();
		}

		for (int i = 1; i < l.size(); i++) {
			String line1 = "";
			List<String> tmplist1 = new ArrayList<String>();
			BufferedReader bufferedReader2 = new BufferedReader(new FileReader(l.get(i)));
			try {
				while ((line1 = bufferedReader2.readLine()) != null) {
					tmplist1.add(line1);
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} finally {
				bufferedReader2.close();
			}
			tmplist = merge(tmplist, tmplist1);
		}
		BufferedWriter fbw = new BufferedWriter(new FileWriter(file));
		try {
			for (String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}
		} finally {
			fbw.close();
		}
	}

	// Merge both lists into a single list by string comparison
	private static List<String> merge(List<String> tmplist, List<String> tmplist1) {
		List<String> mergedList = new ArrayList<String>();
		int i = 0, j = 0, k = 0;

		while (i < tmplist.size() && j < tmplist1.size()) {
			if (tmplist.get(i).substring(0, 10).compareTo(tmplist1.get(j).substring(0, 10)) < 0)
				mergedList.add(k++, tmplist.get(i++));
			else
				mergedList.add(k++, tmplist1.get(j++));
		}

		while (i < tmplist.size())
			mergedList.add(k++, tmplist.get(i++));
		while (j < tmplist1.size())
			mergedList.add(k++, tmplist1.get(j++));
		return mergedList;
	}

	/* Add chunks in the new file and delete the file on exit */
	private static File addChunkInNewFile(List<String> tmplist) throws IOException {
		File newtmpfile = File.createTempFile("UnsortedFile", "Temp File", new File(tempFileLocation));
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
		try {
			for (String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}
		} finally {
			fbw.close();
		}
		return newtmpfile;
	}

	/*
	 * Determine block size and write the block size to list and pass the list
	 * data to addChunkInNewFile method
	 */
	private static List<File> divideFilesInChunk(File file) throws IOException {
		List<File> files = new ArrayList<File>();
		BufferedReader fbr = new BufferedReader(new FileReader(file));
		long sizeoffile = file.length();
		final int MAXTEMPFILES = 2000;
		long blocksize = sizeoffile / MAXTEMPFILES;
		try {
			List<String> tmplist = new ArrayList<String>();
			String line = "";
			try {
				while (line != null) {
					long currentblocksize = 0;// in bytes
					while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) {
						tmplist.add(line);
						currentblocksize += line.length();
					}
					files.add(addChunkInNewFile(tmplist));
					tmplist.clear();
				}
			} catch (EOFException oef) {
				if (tmplist.size() > 0) {
					files.add(addChunkInNewFile(tmplist));
					tmplist.clear();
				}
			}
		} finally {
			fbr.close();
		}
		return files;
	}

	// This method is used to sort the array using quicksort algorithm.
	// It takes left and the right end of the array as two cursors
	private static void quickSort(int left, int right) {

		int temp1 = left, temp2 = right;
		String temp = null;
		String pivot = staticListOfUnsortedFiles.get(left + (right - left) / 2).substring(0, 10);
		while (temp1 <= temp2) {
			while (staticListOfUnsortedFiles.get(temp1).substring(0, 10).compareTo(pivot) < 0) {
				temp1++;
			}
			while (staticListOfUnsortedFiles.get(temp2).substring(0, 10).compareTo(pivot) > 0) {
				temp2--;
			}
			if (temp1 <= temp2) {
				swap(temp1, temp2);
				temp1++;
				temp2--;
			}
		}
		if (left < temp2)
			quickSort(left, temp2);
		if (temp1 < right)
			quickSort(temp1, right);
	}

	// This method is used to swap the values between the two given index
	public static void swap(int temp1, int temp2) {
		String temp = staticListOfUnsortedFiles.get(temp1);
		staticListOfUnsortedFiles.set(temp1, staticListOfUnsortedFiles.get(temp2));
		staticListOfUnsortedFiles.set(temp2, temp);
	}

}
