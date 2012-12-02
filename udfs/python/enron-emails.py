from pig_util import outputSchema

# 
# This is where we write python UDFs (User-Defined Functions) that we can call from pig.
# Pig needs to know the schema of the data coming out of the function, 
# which we specify using the @outputSchema decorator.
#
@outputSchema('example_udf:int')
def example_udf(input_str):
    """
    A simple example function that just returns the length of the string passed in.
    """
    return len(input_str) if input_str else None


from pig_util import outputSchema
@outputSchema('token_strings:chararray')
def tokenize(body):
  te = TokenExtractor()
  tokens = te.tokenize(body)
  no_urls = te.remove_urls(tokens)
  lowers = te.lower(no_urls)
  no_punc = te.remove_punctuation(lowers)
  no_newlines = te.remove_endlines(no_punc)
  no_shorts = te.short_filter(no_newlines)
  return "\t".join(no_shorts)


import nltk
import re
from lepl.apps.rfc3696 import Email, HttpUrl


class TokenExtractor:
  
  def __init__(self):
    self.setup_lepl()
    
  def setup_lepl(self):
    self.is_url = HttpUrl()
    self.is_email = Email()
  
  def tokenize(self, status):
    return nltk.word_tokenize(status)
  
  def lower(self, tokens):
    words = list()
    for token in tokens:
      words.append(token.lower())
    return words
  
  def remove_punctuation(self, tokens):
    punctuation = re.compile(r'[-.@&$#`\'?!,":;()|0-9]')
    words = list()
    for token in tokens:
      word = punctuation.sub("", token)
      if word != "":
        words.append(word)
    return words
  
  def remove_endlines(self, tokens):
    endlines = re.compile(r'\\n')
    tabs = re.compile(r'\\t')
    slashes = re.compile(r'/')
    words = list()
    for token in tokens:
      word = endlines.sub(" ", token)
      word = tabs.sub(" ", word)
      word = slashes.sub(" ", word)
      if word != "":
        words.append(word)
    return words
  
  def short_filter(self, tokens):
    words = list()
    for token in tokens:
      if len(token) > 2:
        words.append(token)
    return words
  
  # Do a regex later
  def remove_urls(self, tokens):
    words = list()
    for token in tokens:
      if self.is_url(token):
        pass
      else:
        words.append(token)
    return words
