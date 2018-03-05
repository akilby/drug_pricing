def lexical_diversity(text):
    return len(set(text)) / len(text)

def percentage_is_word(count, total):
    return 100 * count / total