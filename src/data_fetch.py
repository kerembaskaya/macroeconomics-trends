#! /usr/bin/python
# -*- coding: utf-8 -*-


import urllib2
from bs4 import BeautifulSoup
import os
import Queue
import threading
import sys

__date__ = "14.06.2013"

""" This module fetches the data from http://ideas.repec.org/ """

BASE = 'http://ideas.repec.org'
SITE = 'http://ideas.repec.org/n/nep-mac/'

OUT = "../dataset/"

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
        
        outpath = OUT + directory
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        handle = urllib2.urlopen(url)
        fname = os.path.join(outpath, os.path.basename(url))
        print >> sys.stderr, url, "downloading"
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
    
    print "Download All started"
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
    site = urllib2.urlopen(link)
    soup = BeautifulSoup(site)
    inputs = soup.findAll('input')
    for inp in inputs:
        if 'name' in inp.attrs:
            if inp['name'] == 'url':
                return inp['value']
    raise TypeError("None returned!")


def main():
    pdfs = []
    year_links = get_years_page(2013, 2013)
    for yl in year_links:
        pdfs_date =  find_all_pdfs(yl)
        pdfs.extend(pdfs_date)
    download_all(pdfs)


if __name__ == '__main__':
    main()

