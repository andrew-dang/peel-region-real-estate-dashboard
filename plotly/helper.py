import glob
import sys
from pathlib import Path
import textwrap

def address_hash(address: str):
    h = hash(address)
    if h < 0:
        h = h + sys.maxsize + 1
    
    return h

def select_photoset(address: str):
    photoset = address_hash(address) % 3
    
    return photoset

def get_carousel_items(address: str):
    photoset = (address_hash(address) % 3) + 1
    
    photo_path_list = []
    for photo in sorted(glob.glob(f"./assets/photoset{str(photoset)}/*.jpg")):
        photo_path = Path(photo).as_posix()
        photo_path_list.append(photo_path)
        
    carousel_items = [
        {
            "key": str(i), 
            "src": file_path, 
            "img_class_name": "img_container"
                           } for i,file_path in enumerate(photo_path_list, start=1)]
    
    return carousel_items


def textwrapper(s, width=50):
    return "<br>".join(textwrap.wrap(s,width=width))