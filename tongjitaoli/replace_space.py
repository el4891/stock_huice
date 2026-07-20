import codecs
import multiprocessing
import os
import re
import traceback

import chardet


def convert_to_utf8(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        encoding = chardet.detect(raw_data)['encoding']

    with codecs.open(file_path, 'r', encoding) as f:
        content = f.read()

    with codecs.open(file_path, 'w', 'utf-8') as f:
        f.write(content)


def line_product(filename):
    print(filename)

    convert_to_utf8(filename)

    with open(filename, 'r', encoding='utf-8') as file:
        content = file.readlines()

    content[1] = re.sub(r'\s{2,}', ',', content[1])
    if content[1][0] == ',':
        content[1] = content[1][1:]
    # content = content.replace(' ', ',').replace('\t', ',')

    with open(filename, 'w', encoding='utf-8') as file:
        file.writelines(content)

def data_process(_dir):
    filelist = None
    for i, j, k in os.walk(_dir):
        filelist = k
        print(filelist)

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    for filename_tmp in filelist:
        pool.apply_async(func=line_product, args=(f'{_dir}/{filename_tmp}',))

    pool.close()
    pool.join()

if __name__ == '__main__':
    data_process('day_line')
    data_process('5fen')


