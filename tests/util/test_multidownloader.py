import io
import json

from PIL import Image

from cloudly.util.multidownloader import ImageDownloader, Multidownloader


def test_basic():
    downloader = Multidownloader()
    y = downloader.get('https://httpbin.org/get').decode()
    print()
    y = json.loads(y)
    print(type(y))
    assert 'origin' in y
    assert y['url'] == 'https://httpbin.org/get'
    assert y['args'] == {}


def test_images():
    downloader = ImageDownloader()
    urls = [
        'https://placehold.co/' + u
        for u in [
            '600x400.png?text=Hello\nWorld',
            '800.jpeg?text=Hello+World&font=roboto',
        ]
    ]
    keys = [downloader.submit(u) for u in urls]
    images = [downloader.redeem(k) for k in keys]
    formats = ['PNG', 'JPEG']
    assert all(
        Image.open(io.BytesIO(img)).format == fmt for img, fmt in zip(images, formats)
    )
