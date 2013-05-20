package com.hadoopbbs.searchengine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;

/**
 * 索引文本或HTML文件
 * 
 * @author 石瑜
 * 
 */
public class IndexFiles {

	/** Index all text files under a directory. */
	public static void main(String[] args) {

		IndexFiles indexFiles = new IndexFiles();

		String docsPath = "d:/news";

		String indexPath = "d:/index/news";

		System.out.println(docsPath + " index start ... ");

		long start = System.currentTimeMillis();

		indexFiles.index(docsPath, indexPath);

		long end = System.currentTimeMillis();

		System.out.println("time:\t" + (end - start));

	}

	public IndexFiles() {

	}

	public void index(File docsPath, File indexPath) {

		index(docsPath, indexPath, true);

	}

	/**
	 * 索引文本或HTML文件，指定文档及索引目录，以及是否新建索引
	 * 
	 * @param docsPath
	 *          文档目录
	 * @param indexPath
	 *          索引目录
	 * @param create
	 *          是否新建索引
	 */
	public void index(File docsPath, File indexPath, boolean create) {

		if (docsPath == null || !docsPath.exists() || !docsPath.canRead() || indexPath == null) {

			return;

		}

		// System.out.println("Indexing to directory '" +
		// indexPath.getAbsolutePath() + "'...");
		Directory dir = null;

		try {

			dir = FSDirectory.open(indexPath);

		} catch (IOException ex) {

			ex.printStackTrace();

			return;

		}

		// Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		// Analyzer analyzer = new IKAnalyzer();
		Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_36);

		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_36, analyzer);

		if (create) {

			// Create a new index in the directory, removing any
			// previously indexed documents:
			iwc.setOpenMode(OpenMode.CREATE);

		} else {

			// Add new documents to an existing index:
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

		}

		// Optional: for better indexing performance, if you
		// are indexing many documents, increase the RAM
		// buffer. But if you do this, increase the max heap
		// size to the JVM (eg add -Xmx512m or -Xmx1g):

		iwc.setRAMBufferSizeMB(256);

		IndexWriter writer = null;

		try {

			writer = new IndexWriter(dir, iwc);

			index(writer, docsPath);

		} catch (IOException ex) {

			ex.printStackTrace();

		} finally {

			if (writer != null) {

				// NOTE: if you want to maximize search performance,
				// you can optionally call forceMerge here. This can be
				// a terribly costly operation, so generally it's only
				// worth it when your index is relatively static (ie
				// you're done adding documents to it):

				try {

					writer.forceMerge(1);

				} catch (IOException ex) {

				}

				try {

					writer.close();

				} catch (IOException ex) {

				}

			}

			if (dir != null) {

				try {

					dir.close();

				} catch (IOException ex) {

				}

			}

		}

	}

	/**
	 * Indexes the given file using the given writer, or if a directory is given,
	 * recurses over files and directories found under the given directory.
	 * 
	 * @param writer
	 *          Writer to the index where the given file/dir info will be stored
	 * @param file
	 *          The file to index, or the directory to recurse into to find files
	 *          to index
	 * @throws IOException
	 */
	public void index(IndexWriter writer, File file) throws IOException {

		// do not try to index files that cannot be read
		if (writer == null || file == null || !file.exists() || !file.canRead()) {

			return;

		}

		if (file.isDirectory()) {

			String[] files = file.list();

			// an IO error could occur
			if (files == null) {

				return;

			}

			for (int i = 0; i < files.length; i++) {

				index(writer, new File(file, files[i]));

			}

			files = null;

			return;

		}

		long length = file.length();

		byte[] bytes = new byte[(int) length];

		String value = null;

		FileInputStream fis = null;

		try {

			fis = new FileInputStream(file);

			fis.read(bytes);

			value = new String(bytes, "UTF-8");

		} catch (IOException ex) {

			ex.printStackTrace();

		} finally {

			bytes = null;

			try {

				fis.close();

			} catch (IOException ex) {

			}

		}

		if (value == null) {

			return;

		}

		// 如果是html内容，转换为纯文本内容
		value = Jsoup.parse(value).text();

		// make a new, empty document
		Document doc = new Document();

		// Add the path of the file as a field named "path". Use a
		// field that is indexed (i.e. searchable), but don't tokenize
		// the field into separate words and don't index term frequency
		// or positional information:
		Field pathField = new Field("key", file.getPath(), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);

		pathField.setIndexOptions(IndexOptions.DOCS_ONLY);

		doc.add(pathField);

		// Add the last modified date of the file a field named "modified".
		// Use a NumericField that is indexed (i.e. efficiently filterable
		// with
		// NumericRangeFilter). This indexes to milli-second resolution, which
		// is often too fine. You could instead create a number based on
		// year/month/day/hour/minutes/seconds, down the resolution you
		// require.
		// For example the long value 2011021714 would mean
		// February 17, 2011, 2-3 PM.
		NumericField modifiedField = new NumericField("time");

		modifiedField.setLongValue(file.lastModified());

		doc.add(modifiedField);

		// Add the contents of the file to a field named "contents". Specify a
		// Reader,
		// so that the text of the file is tokenized and indexed, but not
		// stored.
		// Note that FileReader expects the file to be in UTF-8 encoding.
		// If that's not the case searching for special characters will fail.
		doc.add(new Field("value", value, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));

		if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {

			// New index, so we just add the document (no old document can be
			// there):
			writer.addDocument(doc);

		} else {

			// Existing index (an old copy of this document may have been
			// indexed) so
			// we use updateDocument instead to replace the old one matching the
			// exact
			// path, if present:
			writer.updateDocument(new Term("key", file.getPath()), doc);

		}

		doc = null;

	}

	public void index(String docsPath, String indexPath) {

		index(docsPath, indexPath, true);

	}

	public void index(String docsPath, String indexPath, boolean create) {

		if (docsPath == null || indexPath == null) {

			return;

		}

		docsPath = docsPath.trim();

		indexPath = indexPath.trim();

		if (docsPath.length() == 0 || indexPath.length() == 0) {

			return;

		}

		index(new File(docsPath), new File(indexPath), true);

	}

}
