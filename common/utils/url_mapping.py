BEGIN_URL_MAPPING = "-----BEGIN URL MAPPING-----\n"
END_URL_MAPPING   = "-----END URL MAPPING-----\n"

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
            docid_url = line.split(" ")
            url_mapping[int(docid_url[0])] = docid_url[1]

        checkpoint = f.tell()

    return url_mapping, checkpoint
