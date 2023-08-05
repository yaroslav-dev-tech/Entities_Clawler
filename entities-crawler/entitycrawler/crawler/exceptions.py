class NoMatchedPatternError(Exception):

    def __init__(self, url):
        self.url = url
        self.message = "No matched URLPattern for url: [%s]" % url
        super(NoMatchedPatternError, self).__init__(self.message)


class NoSuchScrapperError(Exception):

    def __init__(self, scrapper):
        self.scrapper = scrapper
        self.message = "Can't find scrapper: [%s]" % scrapper
        super(NoMatchedPatternError, self).__init__(self.message)
