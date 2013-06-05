__author__ = 'brock'
import re

def splitString(strToSplit, fragmentLength=160, maxFragments=-1):
    '''returns a list of strings after splitting on max fragment length
    (160 is the max sms length) or the word boundary before that
    '''
    def reverse(l):
        """reverse a list"""
        temp = l[:]
        temp.reverse()
        return temp

    fragments = []

    i = 0
    s = 0
    wordBoundaryRe = re.compile(r'(\s)',re.DOTALL|re.IGNORECASE|re.M)

    # if maxFragments is -1 then make as many fragments as necessary
    while i < maxFragments or maxFragments == -1:
        if maxFragments != -1 and i + 1 == maxFragments:
            # we've reached our max fragments so return the rest of the
            # string as the last fragment.. no matter the length
            fragment = strToSplit[s:]
            fragments.append(fragment)
        else:
            # get the next fragment
            fragment = strToSplit[s:s+fragmentLength]

            if fragment == '':
                break

            # check the end of slice for word boundary
            # we can assume that the last space from end
            # is the word boundary
            m = wordBoundaryRe.search(''.join(reverse(list(fragment))))
            if m is not None:
                fragment = fragment[:len(fragment)-m.start()]
            s = s + len(fragment)
            fragments.append(fragment)

        i += 1

    return fragments
