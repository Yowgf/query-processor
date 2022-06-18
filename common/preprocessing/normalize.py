import nltk

nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

PUNCTUATIONS = set([',', '.', '[', ']', '(', ')', '{', '}', '/', '\\'])
STOPWORDS = set(nltk.corpus.stopwords.words('portuguese') +
                nltk.corpus.stopwords.words('english'))
STEMMERS = [nltk.stem.snowball.PortugueseStemmer(),
            #nltk.stem.snowball.EnglishStemmer(),
]

MIN_CHARS_WORD = 3
MAX_CHARS_WORD = 20

def tokenize(s):
    return nltk.word_tokenize(s)

def normalize_word(word):
    # Stopword removal
    if word in STOPWORDS:
        return None

    # Malformed words removal.
    # Examples: '', ',123', '.hello', '(melt'.
    if len(word) < MIN_CHARS_WORD:
        return None
    if word[0] in PUNCTUATIONS:
        return None

    normalized_word = word
    for stemmer in STEMMERS:
        normalized_word = stemmer.stem(normalized_word)
    if len(normalized_word) > MAX_CHARS_WORD:
        normalized_word = normalized_word[:20]

    return normalized_word

def tokenize_and_normalize(s):
    tokens = tokenize(s)

    normalized_tokens = []
    for token in tokens:
        normalized_token = normalize_word(token)
        if normalized_token != None:
            normalized_tokens.append(normalized_token)

    return normalized_tokens
