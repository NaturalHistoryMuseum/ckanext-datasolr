import re
import sqlparse


def split_words(phrase, quotes=True):
    """ Split a phrase into words

    This will optionally keep "quoted terms" as a
    single word, removing the double quotes.

    Double quotes can be escaped by doubling them
    (ie. ""), and they will be singled in the result.
    (though a word that consists only of a
    doubled double quote is removed), and if the
    number of double quotes is not balanced, then an 
    additional double quote is added at the end of the
    phrase.

    @param phrase: Phrase to split
    @param quotes: If True, then statements
        between double quotes are treated as
        a single word, and the quote symbol is
        removed. If False, quotes are ignored.
    """
    if not quotes:
       return [w for w in phrase.split(' ') if w]
    else:
        nb_q = len(re.sub('[^"]', '', phrase))
        if nb_q%2 == 1:
            phrase += '"'
        parts = re.split(' (?=(?:[^"]|"[^"]*")*$)', phrase)
        words = []
        for w in parts:
            if w.endswith('"'):
                w = w[:-1]
            if w.startswith('"'):
                w = w[1:]
            w = w.replace('""', '"')
            if w:
                words.append(w)
        return words



