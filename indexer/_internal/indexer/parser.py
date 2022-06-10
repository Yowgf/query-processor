from typing import ByteString

from bs4 import BeautifulSoup
from bs4.element import Comment as bs4_comment

class HtmlParser:
    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    def __init__(self, page):
        self._soup = BeautifulSoup(page, "html.parser")
    
    def _is_relevant_text(self, soup_element):
        if soup_element.parent.name in HtmlParser._nontext_tags:
            return False
        if isinstance(soup_element, bs4_comment):
            return False
        if str(soup_element) == ' ' or str(soup_element) == '\n':
            return False
        return True

    def find_text(self):
        texts = self._soup.findAll(text=True)
        
        relevant_words = []
        for element in texts:
            if self._is_relevant_text(element):
                element_str = str(element)
                # Strip new element's string of extra spaces.
                while "  " in element_str:
                    element_str = element_str.replace("  ", " ")
                element_str = element_str.strip()

                # Add new words
                relevant_words.extend(element_str.split(" "))

        return " ".join(relevant_words)

class PlaintextParser:
    def normalize_text(text: ByteString):
        text = text.decode('utf-8')
        text = text.replace("\t", " ")
        text = text.replace("\r", " ")
        text = text.replace("\n", " ")
        while "  " in text:
            text = text.replace("  ", " ")
        return text
