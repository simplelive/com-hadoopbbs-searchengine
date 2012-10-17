package com.hadoopbbs.searchengine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * 搜索文本或HTML文件
 * 
 * @author 石瑜
 * 
 */
public class SearchFiles {

	public int topDocs = 20; // 返回符合条件的最多文件数默认值

	// IndexSearcher HashMap
	public static HashMap<String, IndexSearcher> SEARCHER = new HashMap<String, IndexSearcher>();

	public static void main(String[] args) {

		SearchFiles searchFiles = new SearchFiles();

		String docsPath = "d:/news";

		String indexPath = "d:/index/news";

		System.out.println(docsPath + " index start ... ");

		long start = System.currentTimeMillis();

		String[] keyValues = searchFiles.search(indexPath, "平台");

		long end = System.currentTimeMillis();

		System.out.println("time:\t" + (end - start));

		if (keyValues == null) {

			return;

		}

		System.out.println("count:\t" + keyValues.length);

		for (int i = 0; i < keyValues.length; i++) {

			System.out.println((i + 1) + ":\t" + keyValues[i]);

		}

		System.out.println();

	}

	public IndexSearcher getSearcher(File indexPath) {

		if (indexPath == null) {

			return null;

		}

		String path = indexPath.getAbsolutePath();

		IndexSearcher searcher = SEARCHER.get(path);

		// 索引目录对应的IndexSearcher已经存在
		if (searcher != null) {

			try {

				IndexReader reader = searcher.getIndexReader();

				if (!reader.isCurrent()) { // IndexReader已经改变

					IndexReader _reader = IndexReader.openIfChanged(reader); // 重新打开IndexReader

					if (_reader != null) {

						reader.close();

						searcher.close();

						searcher = new IndexSearcher(_reader);

					}

				}

				return searcher;

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}

		// 新建 IndexReader
		try {

			IndexReader reader = IndexReader.open(FSDirectory.open(indexPath));

			searcher = new IndexSearcher(reader);

			SEARCHER.put(path, searcher);

		} catch (IOException ex) {

			ex.printStackTrace();

			return null;

		}

		return searcher;

	}

	public int getTopDocs() {

		return topDocs;

	}

	public String[] search(File indexPath, String queries) {

		return search(indexPath, queries, topDocs);

	}

	/**
	 * 搜索文件，按指定索引目录，关键字，搜索结果最多文件数
	 * 
	 * @param indexPath
	 *          索引目录
	 * @param queries
	 *          关键字
	 * @param top
	 *          最多文件数
	 * @return
	 */
	public String[] search(File indexPath, String queries, int top) {

		if (indexPath == null || queries == null) {

			return null;

		}

		queries = queries.trim();

		if (queries.length() == 0) {

			return null;

		}

		IndexSearcher searcher = getSearcher(indexPath);

		if (searcher == null) {

			return null;

		}

		// Analyzer analyzer = new IKAnalyzer();
		Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_36);

		QueryParser parser = new QueryParser(Version.LUCENE_36, "value", analyzer);

		Query query = null;

		try {

			query = parser.parse(queries);

		} catch (ParseException ex) {

			ex.printStackTrace();

			try {

				searcher.close();

			} catch (IOException ioex) {

			}

			return null;

		}

		System.out.println("Searching for: " + query.toString());

		top = top < 1 ? topDocs : top;

		TopDocs topDocs = null;

		try {

			topDocs = searcher.search(query, top);

		} catch (IOException ex) {

			ex.printStackTrace();

			try {

				searcher.close();

			} catch (IOException ioex) {

			}

			return null;

		}

		ScoreDoc[] docs = topDocs.scoreDocs;

		Document doc = null;

		String[] keyValues = new String[docs.length];

		for (int i = 0; i < docs.length; i++) {

			try {

				doc = searcher.doc(docs[i].doc);

				keyValues[i] = doc.get("key");

			} catch (IOException ex) {

				ex.printStackTrace();

				try {

					searcher.close();

				} catch (IOException ioex) {

				}

				return null;

			}

		}

		try {

			searcher.close();

		} catch (IOException ioex) {

		}

		return keyValues;

	}

	public String[] search(String indexPath, String queries) {

		return search(indexPath, queries, topDocs);

	}

	public String[] search(String indexPath, String queries, int top) {

		if (indexPath == null) {

			return null;

		}

		indexPath = indexPath.trim();

		if (indexPath.length() == 0) {

			return null;

		}

		return search(new File(indexPath), queries, topDocs);

	}

	public void setTopDocs(int topDocs) {

		this.topDocs = topDocs;

	}

}
