import abc
import json
import re
from typing import List

from metaflowbot.exceptions import MFBException


class MFBParserException(MFBException):
    headline = "Parsing Error"


DEFAULT_TOKENS = [
    "username",
    "flow",
    "runid",
    "tag",
    "start_date",
    "end_date",
    "pattern",
    "artifactname"
]
STATIC_TOKENS = [
    "latest",
    "successful",
    "production"
]

NUMERIC_TOKENS = [
    'size'
]
class BaseParser(metaclass=abc.ABCMeta):
    """BaseParser
    This is a parser created to ensure that basic commands coming to the
    bot via slack can be parsed to extract relevant information
    """
    def __init__(self,\
                tokens=DEFAULT_TOKENS,\
                static_tokens=STATIC_TOKENS,\
                numeric_tokens=NUMERIC_TOKENS) -> None:
        super().__init__()
        self.static_tokens = static_tokens
        self.tokens = tokens
        self.numeric_tokens=numeric_tokens

    def _annotation(self,token_name):
        return f'<{token_name}>'

    @property
    def core_regex(self):
        return None


    @staticmethod
    def _regex_token(token_name):
        # this is created to support :  / - : , _ all numbers all letters
        return f'(?P<{token_name}>[a-zA-Z0-9-\/:,_]+)'

    @staticmethod
    def _static_token(token_name):
        return f'(?P<{token_name}>{token_name})'

    @staticmethod
    def _numeric_token(token_name):
        return f'(?P<{token_name}>[0-9]+)'

    @staticmethod
    def make_regex(pattern):
        return re.compile(pattern, re.IGNORECASE)

    def _transform_regex(self,sentence):
        # Create a regex string to compile
        curr_sent = sentence
        for token_name in self.tokens+self.static_tokens+self.numeric_tokens:
            replacement_token = self._regex_token(token_name)
            if token_name in self.static_tokens:
                replacement_token = self._static_token(token_name)
            elif token_name in self.numeric_tokens:
                replacement_token = self._numeric_token(token_name)
            curr_sent = curr_sent.replace(\
                self._annotation(token_name),\
                replacement_token
            )
        return curr_sent

    def _make_regex_sentences(self,sentences:List[str]):
        new_sent_set = list()
        for sentence in sentences:
            curr_sent = self._transform_regex(sentence)
            if curr_sent !=sentence:
                new_sent_set.append(curr_sent)
        return new_sent_set


class LanguageParser(BaseParser):
    """[LanguageParser]
    Parses sentences to create regex like strings for
    fast matching
    Example Sentences :
    ----------------------------
        "list flows run by <username>" \n
        "list flows by <username> running in <prod>" \n
        "list flows by <username> that ran in <prod>" \n
        "list flow that ran from <end_date> to <start_date>" \n
        "list flows which have a tag <tag>" \n
    Running LanguageParser :
    ----------------------------
    ```python
    parse = LanguageParser(["list flows run by <username>","list flows by <username> running in <prod>"])
    parse("list flows run by valaydave")
    ```
    """

    def __init__(self,sentences:List[str],tokens=DEFAULT_TOKENS,static_tokens=STATIC_TOKENS,) -> None:
        super().__init__(tokens=tokens,static_tokens=static_tokens)
        self.sentences =sentences
        self._regex_parsers = [self.make_regex(x)
                            for x in self._make_regex_sentences(sentences)]

    @property
    def core_regex(self):
        return self._make_regex_sentences(self.sentences)


    def __call__(self,sentence):
        matches = list(filter(None, [p.match(sentence) for p in self._regex_parsers]))
        if len(matches) > 0:
            return matches[0].groupdict()
        else:
            return None
