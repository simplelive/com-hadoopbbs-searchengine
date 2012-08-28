package com.hadoopbbs.searchengine;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * 
 * 搜索数据库表
 * 
 * @author 石瑜
 * 
 */
public class SearchTable {

	public final static int TOP_DOCS = 100; // 返回符合条件的最多记录数默认值

	public static void main(String[] args) throws Exception {

		System.out.println("search start ...");

		SearchTable search = new SearchTable();

		String indexBase = "d:/index";

		String table = "article";

		String queries = "北京欢迎尔";

		String[] colNames = { "title", "content" };

		String keyName = "id";

		long start = System.currentTimeMillis();

		String[] keyValues = search.search(indexBase, table, queries, colNames, keyName, false);

		long end = System.currentTimeMillis();

		System.out.println("time:\t" + (end - start));

		System.out.println("count:\t" + keyValues.length);

		System.out.print(keyName + ":\t");

		for (int i = 0; i < keyValues.length; i++) {

			System.out.print(keyValues[i]);

			System.out.print(",");

		}

		System.out.println();

	}

	public SearchTable() {

	}

	public String[] search(File indexBase, String table, String queries, String[] colNames, String keyName) throws IOException, ParseException {

		return search(indexBase, table, queries, colNames, keyName, false);

	}

	public String[] search(File indexBase, String table, String queries, String[] colNames, String keyName, boolean and) throws IOException, ParseException {

		return search(indexBase, table, queries, colNames, keyName, and, TOP_DOCS);

	}

	/**
	 * 搜索表，按指定的上级索引目录，表名，搜索关键字，列名数组，主键名，是否全部列都包含关键字(AND操作)，最多返回结果数
	 * 
	 * @param indexBase
	 *          上级索引目录
	 * @param table
	 *          表名
	 * @param queries
	 *          搜索关键字
	 * @param colNames
	 *          列名数组
	 * @param keyName
	 *          主键名
	 * @param and
	 *          是否全部列都包含关键字(AND操作)
	 * @param top
	 *          最多返回结果数
	 * @return 键值数组，或null
	 * @throws IOException
	 * @throws ParseException
	 */
	public String[] search(File indexBase, String table, String queries, String[] colNames, String keyName, boolean and, int top) throws IOException, ParseException {

		if (indexBase == null || table == null || table.length() == 0 || queries == null || queries.length() == 0 || colNames == null || colNames.length == 0 || keyName == null || keyName.length() == 0) {

			return null;

		}

		File indexPath = new File(indexBase, table);

		IndexReader reader = IndexReader.open(FSDirectory.open(indexPath));

		IndexSearcher searcher = new IndexSearcher(reader);

		// Analyzer analyzer = new IKAnalyzer();
		Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_36);

		Occur[] clauses = new Occur[colNames.length];

		for (int i = 0; i < clauses.length; i++) {

			if (and) {

				clauses[i] = Occur.MUST;

			} else {

				clauses[i] = Occur.SHOULD;

			}

			colNames[i] = colNames[i].toLowerCase();

		}

		Query query = MultiFieldQueryParser.parse(Version.LUCENE_36, queries, colNames, clauses, analyzer);

		// System.out.println("Searching for: " + query.toString());

		top = top < 1 ? TOP_DOCS : top;

		TopDocs topDocs = searcher.search(query, top);

		ScoreDoc[] docs = topDocs.scoreDocs;

		Document doc = null;

		String[] keyValues = new String[docs.length];

		keyName = keyName.toLowerCase();

		for (int i = 0; i < docs.length; i++) {

			doc = searcher.doc(docs[i].doc);

			keyValues[i] = doc.get(keyName);

		}

		return keyValues;

	}

	public String[] search(String indexBase, String table, String queries, String[] colNames, String keyName) throws CorruptIndexException, IOException, ParseException {

		return search(indexBase, table, queries, colNames, keyName, false);

	}

	public String[] search(String indexBase, String table, String queries, String[] colNames, String keyName, boolean and) throws CorruptIndexException, IOException, ParseException {

		return search(indexBase, table, queries, colNames, keyName, and, TOP_DOCS);

	}

	public String[] search(String indexBase, String table, String queries, String[] colNames, String keyName, boolean and, int top) throws CorruptIndexException, IOException, ParseException {

		return search(new File(indexBase), table, queries, colNames, keyName, and, top);

	}

}
