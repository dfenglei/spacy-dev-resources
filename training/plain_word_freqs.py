from __future__ import unicode_literals

import codecs
import glob
import json
import os
import shutil
import tempfile
from collections import Counter
from multiprocessing import Pool

import plac
import six
from tqdm import tqdm

TMP_DIR = tempfile.mkdtemp()


def count_words(fpath):
    fname = os.path.split(fpath)[-1]
    outfpath = os.path.join(TMP_DIR, fname)
    with codecs.open(fpath, encoding="utf8") as f:
        words = f.read().split()
        counter = dict(Counter(words))
        json.dump(counter, open(outfpath, "w"))
    return outfpath


def main(input_glob, out_loc, workers=4):
    p = Pool(processes=workers)
    freq_path = p.map(count_words, glob.iglob(input_glob))
    df_counts = Counter()
    word_counts = Counter()
    for fp in tqdm(freq_path):
        with open(fp) as f:
            wc = json.load(f)
            df_counts.update(wc.keys())
            word_counts.update(wc)
    with codecs.open(out_loc, "w", encoding="utf8") as f:
        for word, df in six.iteritems(df_counts):
            f.write(u"{freq}\t{df}\t{word}\n".format(word=repr(word), df=df, freq=word_counts[word]))
    shutil.rmtree(TMP_DIR)


if __name__ == "__main__":
    plac.call(main)
