from xml.etree import ElementTree
from collections import defaultdict
import json
import requests
import re

ROOT_TAG = "ding"


def dingFormatter():
    e = ElementTree.Element(ROOT_TAG)
    e.text = ".*?"
    return ElementTree.tostring(e, "unicode")


DingCompiler = re.compile(dingFormatter())


def makeNotify(message, titles, channels):
    root = ElementTree.Element(ROOT_TAG)

    for title in titles:
        etitle = ElementTree.SubElement(root, "title")
        etitle.text = title

    for channel in channels:
        echannel = ElementTree.SubElement(root, "channel")
        echannel.text = channel
    
    emessage = ElementTree.SubElement(root, "message")
    emessage.text = message

    return ElementTree.tostring(root, "unicode")

