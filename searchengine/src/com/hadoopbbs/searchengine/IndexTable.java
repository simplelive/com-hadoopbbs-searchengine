package com.hadoopbbs.searchengine;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;

import com.hadoopbbs.database.Database;

/**
 * 
 * 索引数据库表
 * 
 * @author 石瑜
 * 
 */

public class IndexTable {

	public final static int MAX_ROWS = 10000; // 进行索引时，查询数据库每次最多返回记录数

	public static void main(String[] args) throws IOException, SQLException {

		IndexTable index = new IndexTable();

		String indexBase = "d:/index";

		String table = "article";

		String keyName = "id";

		String[] colNames = { "title", "content" };

		System.out.println(table + " index start ...");

		long start = System.currentTimeMillis();

		index.index(indexBase, table, colNames, keyName);

		long end = System.currentTimeMillis();

		System.out.println("time:\t" + (end - start));

	}

	Database db = null;

	public IndexTable() {

		db = new Database();

	}

	public void index(File indexBase, String table, String[] colNames, String keyName) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, true);

	}

	public void index(File indexBase, String table, String[] colNames, String keyName, boolean create) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, create, null);

	}

	public void index(File indexBase, String table, String[] colNames, String keyName, boolean create, String keyStart) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, create, keyStart, 0);

	}

	/**
	 * 索引表，根据指定的上级索引目录，表名，主键名，需要索引的列名数组， 主键开始值，索引记录总数
	 * 
	 * @param indexBase
	 *          上级索引目录
	 * @param table
	 *          表名
	 * @param keyName
	 *          主键名
	 * @param colNames
	 *          需要索引的列名数组
	 * @param create
	 *          新建或更新索引
	 * @param keyStart
	 *          主键开始值
	 * @param rowCount
	 *          索引记录总数
	 * @throws IOException
	 * @throws SQLException
	 */
	public void index(File indexBase, String table, String[] colNames, String keyName, boolean create, String keyStart, int rowCount) throws IOException, SQLException {

		if (indexBase == null || table == null || table.length() == 0 || keyName == null || keyName.length() == 0 || colNames == null || colNames.length == 0) {

			return;

		}

		File indexPath = new File(indexBase, table);

		Directory dir = FSDirectory.open(indexPath);

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
		//

		iwc.setRAMBufferSizeMB(256);

		IndexWriter writer = new IndexWriter(dir, iwc);

		index(writer, table, colNames, keyName, keyStart, rowCount);

		writer.close();

	}

	/**
	 * 索引记录，根据IndexWriter，键名，键值，列名数组，列值数组
	 * 
	 * @param writer
	 *          IndexWriter
	 * @param keyName
	 *          键名
	 * @param keyValue
	 *          键值
	 * @param colNames
	 *          列名数组
	 * @param colValues
	 *          列值数组
	 * @throws IOException
	 */
	public void index(IndexWriter writer, String keyName, String keyValue, String[] colNames, String[] colValues) throws IOException {

		if (writer == null || keyName == null || keyName.length() == 0 || keyValue == null || keyValue.length() == 0 || colNames == null || colNames.length == 0 || colValues == null || colValues.length == 0) {

			return;

		}

		Document doc = new Document();

		// KEY
		Field keyField = new Field(keyName.toLowerCase(), keyValue, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);

		keyField.setIndexOptions(IndexOptions.DOCS_ONLY);

		doc.add(keyField);

		for (int i = 0; i < colNames.length; i++) {

			if (colNames[i] != null && colValues[i] != null) {

				// 如果是html内容，转换为纯文本内容
				colValues[i] = Jsoup.parse(colValues[i]).text();

				// System.out.println(colValues[i]);

				// 字段内容
				Field colField = new Field(colNames[i].toLowerCase(), colValues[i], Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);

				doc.add(colField);

			}

		}

		// 更新索引
		if (writer.getConfig().getOpenMode() == OpenMode.CREATE) { // 添加

			writer.addDocument(doc);

		} else { // 更新

			writer.updateDocument(new Term(keyName, keyValue), doc);

		}

	}

	public void index(IndexWriter writer, String table, String[] colNames, String keyName) throws IOException, SQLException {

		index(writer, table, colNames, keyName, null, 0);

	}

	public void index(IndexWriter writer, String table, String[] colNames, String keyName, String keyStart) throws IOException, SQLException {

		index(writer, table, colNames, keyName, keyStart, 0);

	}

	/**
	 * 索引表，根据指定的IndexWriter，表名，主键名，列名数组， 主键开始值，索引记录总数
	 * 
	 * @param writer
	 *          IndexWriter
	 * @param table
	 *          表名
	 * @param keyName
	 *          主键名
	 * @param colNames
	 *          需要索引的列名数组
	 * @param keyStart
	 *          主键开始值
	 * @param rowCount
	 *          索引记录总数
	 * @throws IOException
	 * @throws SQLException
	 */
	public void index(IndexWriter writer, String table, String[] colNames, String keyName, String keyStart, int rowCount) throws IOException, SQLException {

		if (writer == null || table == null || table.length() == 0 || keyName == null || keyName.length() == 0 || colNames == null || colNames.length == 0) {

			return;

		}

		StringBuilder sql = new StringBuilder("SELECT ");

		sql.append(keyName);

		for (int i = 0; i < colNames.length; i++) {

			sql.append(",");

			sql.append(colNames[i]);

		}

		sql.append(" FROM ");

		sql.append(table);

		if (keyStart != null && keyStart.length() > 0) {

			sql.append(" WHERE ");

			sql.append(keyName);

			sql.append(" >= ");

			sql.append(keyStart);

		}

		sql.append(" ORDER BY ");

		sql.append(keyName);

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = db.getConnection();

			ps = conn.prepareStatement(sql.toString());

			ps.setMaxRows(MAX_ROWS + 1);

			rs = ps.executeQuery();

			String keyValue = null;

			String[] colValues = new String[colNames.length];

			int count = 0;

			while (rs.next()) {

				keyValue = String.valueOf(rs.getObject(keyName));

				for (int i = 0; i < colNames.length; i++) {

					colValues[i] = String.valueOf(rs.getObject(colNames[i]));

				}

				index(writer, keyName, keyValue, colNames, colValues);

				count++;

				if (count == MAX_ROWS || (rowCount > 0 && count == rowCount)) {

					break;

				}

			}

			if (count == MAX_ROWS) {

				if (rs.next()) {

					keyValue = String.valueOf(rs.getObject(keyName));

					rowCount = rowCount > 0 ? rowCount - count : rowCount;

					index(writer, table, colNames, keyName, keyValue, rowCount);

				}

			}

		} catch (SQLException ex) {

			throw ex;

		} finally {

			db.close(rs, ps, conn);

		}

	}

	public void index(String indexBase, String table, String[] colNames, String keyName) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, true);

	}

	public void index(String indexBase, String table, String[] colNames, String keyName, boolean create) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, create, null);

	}

	public void index(String indexBase, String table, String[] colNames, String keyName, boolean create, String keyStart) throws IOException, SQLException {

		index(indexBase, table, colNames, keyName, create, keyStart, 0);

	}

	public void index(String indexBase, String table, String[] colNames, String keyName, boolean create, String keyStart, int rowCount) throws IOException, SQLException {

		index(new File(indexBase), table, colNames, keyName, create, keyStart, rowCount);

	}

}
