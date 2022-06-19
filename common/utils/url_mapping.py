BEGIN_URL_MAPPING = "-----BEGIN URL MAPPING-----\n"
END_URL_MAPPING   = "-----END URL MAPPING-----\n"

class UrlMapping:
    def __init__(self, url_mapping):
        self._m = url_mapping

    def get_doc_len(self, docid):
        return self._m[docid][0]

    def get_url(self, docid):
        return self._m[docid][1]

def read_url_mapping(fpath, checkpoint):
    url_mapping = {}
    with open(fpath, "r") as f:
        f.seek(checkpoint)

        assert f.readline() == BEGIN_URL_MAPPING
        s = ""
        newline = f.readline()
        while newline != END_URL_MAPPING:
            s += newline
            newline = f.readline()
        lines = s.rstrip().split("\n")
        for line in lines:
            docid_values = line.split(" ")
            docid = int(docid_values[0])
            doc_len = int(docid_values[1])
            url = docid_values[2]
            url_mapping[docid] = (doc_len, url)

        checkpoint = f.tell()

    return UrlMapping(url_mapping), checkpoint
