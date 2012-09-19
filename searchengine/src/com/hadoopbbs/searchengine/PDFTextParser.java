package com.hadoopbbs.searchengine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

/**
 * PDF工具，转换PDF为纯文本
 * 
 * @author 石瑜
 * 
 */

public class PDFTextParser {

	public static void main(String[] args) {

		System.out.println("start ...");

		PDFTextParser p = new PDFTextParser();

		String file = "D:/Docs/Lucene/Google_三大论文中文版.pdf";

		long start = System.currentTimeMillis();

		String text = p.toText(file);

		long end = System.currentTimeMillis();

		System.out.println(text);

		System.out.println("time:\t" + (end - start));

	}

	public PDFTextParser() {

	}

	public String toText(File file) {

		if (file == null || !file.exists() || !file.canRead() || !file.isFile()) {

			return null;

		}

		PDFParser parser = null;

		COSDocument cos = null;

		PDDocument doc = null;

		PDFTextStripper stripper = null;

		String text = null;

		try {

			parser = new PDFParser(new FileInputStream(file));

			parser.parse();

			cos = parser.getDocument();

			doc = new PDDocument(cos);

			stripper = new PDFTextStripper();

			text = stripper.getText(doc);

		} catch (IOException ex) {

			ex.printStackTrace();

		} finally {

			if (doc != null) {

				try {

					doc.close();

				} catch (IOException ex) {

				}

			}

			if (cos != null) {

				try {

					cos.close();

				} catch (IOException ex) {

				}

			}

		}

		return text;

	}

	public String toText(String file) {

		if (file == null) {

			return null;

		}

		file = file.trim();

		if (file.length() == 0) {

			return null;

		}

		return toText(new File(file));

	}

}
