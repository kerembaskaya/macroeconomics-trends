#! /usr/bin/python
# -*- coding: utf-8 -*-


import urllib2
from bs4 import BeautifulSoup
import os
import Queue
import threading
import sys
import random

__date__ = "14.06.2013"

""" This module fetches the data from http://ideas.repec.org/ """

BASE = 'http://ideas.repec.org'
SITE = 'http://ideas.repec.org/n/nep-mac/'

OUT = "../dataset/pdf/"

class Downloader(threading.Thread):
 
    def __init__(self, queue):
        global counter
        threading.Thread.__init__(self)
        self.queue = queue
 
    def run(self):
        while True:
            url, directory = self.queue.get()
            url = get_pdf_file_link(url)
            self.download_file(url, directory)
            self.queue.task_done()

    def download_file(self, url, directory):


        basename = os.path.basename(url)
        if not (basename.endswith('pdf') or len(basename) == 0):
            print >> sys.stderr, url, '\t', directory
            return

        outpath = OUT + directory
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        try:
            handle = urllib2.urlopen(url)
        except: #404
            return

        if len(basename) == 0:
            r = random.random()
            basename = str(hash(r))

        fname = os.path.join(outpath, basename)
        with open(fname, "wb") as f:
            while True:
                chunk = handle.read(1024)
                if not chunk: break
                f.write(chunk)

def get_years_page(start_year, finish_year):
    years = map(str, range(start_year, finish_year+1))
    site = urllib2.urlopen(SITE)
    soup = BeautifulSoup(site)
    links = soup.findAll('a')
    href_links = [link['href'] for link in links if 'href' in link.attrs]
    year_links = [SITE+hlinks for hlinks in href_links if hlinks.split('-')[0] in years]
    return year_links


def download_all(pdfs):
    
    print "PDF Download started"
    queue = Queue.Queue()
 
    # create a thread pool and give them a queue
    for i in range(5):
        t = Downloader(queue)
        t.setDaemon(True)
        t.start()
 
    # give the queue some data
    for url in pdfs:
        queue.put(url)
 
    # wait for the queue to finish
    queue.join()


def find_all_pdfs(link):

    date = link.split('/')[-1][:-5]
    site = urllib2.urlopen(link)
    soup = BeautifulSoup(site)
    links = soup.findAll('a')
    href_links = [link['href'] for link in links if 'href' in link.attrs]
    links_dates = [(BASE + hlinks, date) for hlinks in href_links \
                                                if hlinks.startswith('/p/')]
    return links_dates

def get_pdf_file_link(link):
    try:
        site = urllib2.urlopen(link)
    except: #404
        return ""
    soup = BeautifulSoup(site)
    inputs = soup.findAll('input')
    for inp in inputs:
        if 'name' in inp.attrs:
            if inp['name'] == 'url':
                return inp['value']
    return ""


def main():
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    pdfs = []
    year_links = get_years_page(start, end)
    print "total issues: ", len(year_links)  
    for yl in year_links:
        #print yl
        pdfs_date =  find_all_pdfs(yl)
        pdfs.extend(pdfs_date)
    print "#of PDF", len(pdfs)
    download_all(pdfs)


if __name__ == '__main__':
    main()

