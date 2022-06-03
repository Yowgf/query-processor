def read_index(infpath):
    index = {}
    index_str = open(infpath, 'r').read()
    inverted_lists = [entry for entry in index_str.split("\n") if entry != '']
    for inverted_list in inverted_lists:
        split_by_space = inverted_list.split(" ")
        if len(split_by_space) <= 1:
            continue

        word = split_by_space[0]
        index[word] = []
        for docfreq_str in split_by_space[1:]:
            docfreq_split = docfreq_str.split(",")
            docfreq = (int(docfreq_split[0]), int(docfreq_split[1]))
            index[word].append(docfreq)
    return index

def write_index(index, outfpath):
    with open(outfpath, 'a') as outf:
        for word in index:
            outf.write(word)
            inverted_list = index[word]
            for entry in inverted_list:
                outf.write(f" {entry[0]},{entry[1]}")
            outf.write("\n")

def merge_indexes(index1, index2):
    merged_index = {}
    for word in index1:
        if word in index2:
            merged_index[word] = sorted(index1[word] + index2[word])
        else:
            merged_index[word] = index1[word]
    return merged_index

def is_useful_warcio_record(record):
    return (record.rec_type == 'response' and
            record.http_headers != None and
            record.http_headers.get_header('Content-Type') == 'text/html')

def get_warcio_record_url(record):
    return record.rec_headers.get_header('WARC-Target-URI')
