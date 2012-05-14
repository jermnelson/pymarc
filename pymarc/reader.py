from io import StringIO,TextIOWrapper,BytesIO,BufferedReader
import logging,sys,traceback
from pymarc import Record
from pymarc.exceptions import RecordLengthInvalid
import collections

class Reader(object):
    """
    A base class for all iterating readers in the pymarc package. 
    """
    def __iter__(self):
        return self

class MARCReader(Reader):

    def __init__(self, marc_target, **kwargs):
        """
        An iterator class for reading a file of MARC21 records using Python 3 unicode 
        and byte support. Following the concept of Unicode "sandwich" approach with 
        explicit MARC file handling heuristics.
  
        :param marc_target: "file" like object or raw MARC byte-stream
        :param to_unicode: Depreciated
        :param force_utf8: Depreciated
        :param hide_utf8_warnings: Boolean, shows any byte-to-unicode warnings
        :param utf8_handling: One of these values ('strict', 'replace', 'xmlcharrefreplace' and 'ignore')
        """
        super(MARCReader, self).__init__()
        if 'utf8_handling' in kwargs:
            self.utf8_handling = kwargs.get('utf8_handling')
        else:
            self.utf8_handling = 'strict'
        if 'hide_utf8_warnings' in kwargs:
            self.hide_utf8_warnings = kwargs.get('hide_utf8_warnings')
        else:
            self.hide_utf8_warnings = False
        if (hasattr(marc_target, "read") and isinstance(marc_target.read, collections.Callable)):
            if type(marc_target) is BufferedReader:
                self.byte_stream = marc_target
            else:
                # Gets name of marc_target, close file handle and opens in binary mode
                filehandle_name = marc_target.name
                marc_target.close()
                self.byte_stream = open(filehandle_name,'rb')
        else:
            if hasattr(marc_target,'buffer'):
                self.byte_stream = BufferedReader(marc_target)
            else:
                raw_bytes = b'' + marc_target.encode(encoding='ascii',
                                                     errors=self.utf8_handling)
                self.byte_stream = BytesIO(raw_bytes)


    def __next__(self):
        first5 = self.byte_stream.read(5)
        if not first5:
            raise StopIteration
        if len(first5) < 5:
            raise RecordLengthInvalid
        length = int(first5)
        chunk = self.byte_stream.read(length - 5)
        chunk = first5 + chunk
        try:
            chunk = chunk.decode(encoding='utf-8',
                                 errors=self.utf8_handling)
        except TypeError:
            logging.error("TypeError chunk type is %s" % type(chunk))
        except:
            logging.error("Cannot iterate, error=%s" % sys.exc_info()[0])
        try:
            record = Record(chunk, 
                            hide_utf8_warnings=self.hide_utf8_warnings,
                            utf8_handling=self.utf8_handling)
            return record
        #except IndexError:
        #    logging.error("Chunk length=%s type=%s" % (len(chunk),type(chunk)))             
        except:
            traceback.print_exc(file=sys.stderr)
     
class DepreciatedMARCReader(Reader):
    """
    An iterator class for reading a file of MARC21 records. 

    Simple usage:

        from pymarc import MARCReader

        ## pass in a file object
        reader = MARCReader(file('file.dat'))
        for record in reader:
            ...

        ## pass in marc in transmission format 
        reader = MARCReader(rawmarc)
        for record in reader:
            ...

    If you would like to have your Record object contain unicode strings
    use the to_unicode parameter:

        reader = MARCReader(file('file.dat'), to_unicode=True)

    This will decode from MARC-8 or UTF-8 depending on the value in the 
    MARC leader at position 9. 
    
    If you find yourself in the unfortunate position of having data that 
    is utf-8 encoded without the leader set appropriately you can use 
    the force_utf8 parameter:

        reader = MARCReader(file('file.dat'), to_unicode=True,
            force_utf8=True)
    
    If you find yourself in the unfortunate position of having data that is 
    mostly utf-8 encoded but with a few non-utf-8 characters, you can also use
    the utf8_handling parameter, which takes the same values ('strict', 
    'replace', and 'ignore') as the Python Unicode codecs (see 
    http://docs.python.org/library/codecs.html for more info).

    """
    def __init__(self, marc_target, to_unicode=False, force_utf8=False,
        hide_utf8_warnings=False, utf8_handling='strict'):
        """
        The constructor to which you can pass either raw marc or a file-like
        object. Basically the argument you pass in should be raw MARC in 
        transmission format or an object that responds to read().
        
        """
        super(MARCReader, self).__init__()
        self.to_unicode = to_unicode
        self.force_utf8 = force_utf8
        self.hide_utf8_warnings = hide_utf8_warnings
        self.utf8_handling = utf8_handling
        if hasattr(marc_target,"buffer"):
            ##self.file_handle = BufferedReader(marc_target.buffer)
        ##if (hasattr(marc_target, "read") and isinstance(marc_target.read, collections.Callable)):
            text_wrapper = TextIOWrapper(marc_target)
        ##    self.file_handle = BufferedReader(marc_target.buffer)
        ##else: 
        ##    self.file_handle = StringIO(marc_target)
        else:
            text_wrapper = TextIOWrapper(marc_target)
        self.file_handle = BufferedReader(text_wrapper.buffer)
            #self.file_handle = BytesIO(marc_target)
        logging.error("SELF File handle type is %s" % type(self.file_handle))

    def __next__(self):
        """
        To support iteration. 
        """
        first5 = self.file_handle.read(5)
        if not first5:
            raise StopIteration
        if len(first5) < 5:
            raise RecordLengthInvalid

        length = int(first5)
        chunk = self.file_handle.read(length - 5)
        chunk = first5 + chunk
        record = Record(chunk, 
                        to_unicode=self.to_unicode,
                        force_utf8=self.force_utf8,
                        hide_utf8_warnings=self.hide_utf8_warnings,
                        utf8_handling=self.utf8_handling)
        return record 

def map_records(f, *files):
    """
    Applies a given function to each record in a batch. You can
    pass in multiple batches.

    >>> def print_title(r): 
    >>>     print r['245']
    >>> 
    >>> map_records(print_title, file('marc.dat'))
    """
    for file in files:
        list(map(f, MARCReader(file)))

