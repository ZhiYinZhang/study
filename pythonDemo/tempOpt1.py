#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/26 17:27
import json
from pdfminer.pdfinterp import PDFResourceManager,PDFPageInterpreter,PDFTextExtractionNotAllowed
from pdfminer.layout import LTTextBoxHorizontal,LAParams
from pdfminer.pdfparser import PDFParser,PDFDocument
from pdfminer.converter import PDFPageAggregator


pdf_file="E:\资料\hadoop\大规模分布式存储系统：原理解析与架构实战-页面\page-4.pdf"
result_file="E:\资料\hadoop\\test\page-4.txt"
#
origin_pdf_file=open(pdf_file,"rb")

parser=PDFParser(origin_pdf_file)

doc=PDFDocument()

parser.set_document(doc)
doc.set_parser(parser)

doc.initialize()

page_start=0
page_end=1

if not doc.is_extractable:
    raise PDFTextExtractionNotAllowed
else:
    srcmgr=PDFResourceManager()

    device=PDFPageAggregator(rsrcmgr=srcmgr,laparams=LAParams())

    interpreter=PDFPageInterpreter(srcmgr,device)

    pages=list(doc.get_pages())
    print(len(pages))
    if page_end==0:
        page_end=len(pages)

    for i in range(page_start,page_end):
        interpreter.process_page(pages[i])


        layout=device.get_result()


        for x in layout:
            if isinstance(x,LTTextBoxHorizontal):
                with open(result_file,'w',encoding='utf-8') as f:
                    print(results)
                    results=x.get_text()
                    f.write(results+'\n')

origin_pdf_file.close()