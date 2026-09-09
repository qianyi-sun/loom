"""Bounded, exact-identity S3 ListObjectsV2 response parsing."""

import importlib
from xml.sax.saxutils import escape

import pytest


def _module():
    return importlib.import_module("loom_task_image_authority.bundle_s3_listing")


def _xml(*, contents=None, fields="", truncated="false", prefix="revision%2F", count=None):
    if contents is None:
        contents = [("revision%2Fa%20space%2B%25.toml", "7"), ("revision%2Fliteral%252Fkey", "0")]
    count = len(contents) if count is None else count
    entries = "".join(f"<Contents><Key>{escape(key)}</Key><Size>{size}</Size><ETag>opaque</ETag></Contents>" for key, size in contents)
    return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>loom-bundles</Name><Prefix>{prefix}</Prefix><MaxKeys>2</MaxKeys><KeyCount>{count}</KeyCount><EncodingType>url</EncodingType><IsTruncated>{truncated}</IsTruncated>{entries}{fields}</ListBucketResult>').encode()


def _parse(payload=None, **changes):
    options = dict(expected_bucket="loom-bundles", prefix="revision/", maximum_keys=2, continuation_token=None, url_encoding="percent")
    options.update(changes)
    return _module().parse_list_objects_v2(_xml() if payload is None else payload, **options)


def test_decodes_keys_once_and_preserves_exact_sizes():
    page = _parse()
    assert [(obj.key, obj.size_bytes) for obj in page.objects] == [
        ("revision/a space+%.toml", 7), ("revision/literal%2Fkey", 0),
    ]
    assert page.next_token is None


@pytest.mark.parametrize("encoding,expected", [("percent", "revision/a+space+%.toml"), ("form", "revision/a space+%.toml")])
def test_plus_decoding_requires_explicit_service_encoding(encoding, expected):
    page = _parse(_xml(contents=[("revision%2Fa+space%2B%25.toml", "7")]), url_encoding=encoding)
    assert page.objects[0].key == expected


def test_form_encoded_prefix_is_decoded_exactly_once():
    page = _parse(_xml(prefix="revision+space%2B%2F", contents=[("revision+space%2B%2Fa%252F", "7")]), prefix="revision space+/", url_encoding="form")
    assert page.objects[0].key == "revision space+/a%2F"


@pytest.mark.parametrize("options", [
    {"url_encoding": "guess"}, {"url_encoding": None}, {"url_encoding": []},
    {"maximum_keys": True}, {"maximum_keys": 0}, {"maximum_keys": 1001},
    {"prefix": "revision\n/"}, {"prefix": "revision\x7f/"},
    {"prefix": "x" * 1024 + "/"}, {"prefix": "revision"},
    {"continuation_token": "x" * 4097}, {"continuation_token": "token\n"},
    {"continuation_token": ""}, {"continuation_token": "é"},
], ids=lambda _: "invalid-request")
def test_rejects_invalid_request_before_parser_allocation(monkeypatch, options):
    _module()

    def unexpected(*args, **kwargs):
        pytest.fail("invalid request must be refused before XML parser allocation")

    monkeypatch.setattr("xml.parsers.expat.ParserCreate", unexpected)
    with pytest.raises(RuntimeError):
        _parse(**options)


@pytest.mark.parametrize("payload", [
    _xml(fields="<NextContinuationToken>" + "x" * 4097 + "</NextContinuationToken>", truncated="true"),
    _xml().replace(b"opaque", b"x" * 8193),
    _xml().replace(b"<ETag>opaque</ETag>", b"<Owner><ID><Nested><Deep>x</Deep></Nested></ID></Owner>"),
    _xml().replace(b"<ETag>opaque</ETag>", b"<Owner>" + b"<ID/>" * 16384 + b"</Owner>"),
    _xml().replace(b"<ETag>opaque</ETag>", b"<Owner>" + b"<ID>" + b"x&#32;" * 32769 + b"</ID></Owner>"),
], ids=lambda _: "structural-limit")
def test_rejects_bounded_structure_and_token_overflows(payload):
    with pytest.raises(RuntimeError):
        _parse(payload)


def test_truncated_page_has_bounded_non_replayed_continuation():
    page = _parse(_xml(truncated="true", fields="<NextContinuationToken>next-token</NextContinuationToken>"))
    assert page.next_token == "next-token"
    page = _parse(_xml(fields="<ContinuationToken>prior-token</ContinuationToken>"), continuation_token="prior-token")
    assert page.next_token is None


@pytest.mark.parametrize("payload", [
    _xml().replace(b"loom-bundles", b"other-bundles"),
    _xml(prefix="foreign%2F"), _xml(count=1),
    _xml().replace(b"<MaxKeys>2", b"<MaxKeys>3"),
    _xml().replace(b"<EncodingType>url</EncodingType>", b""),
    _xml(contents=[("revision%2Fa", "1"), ("revision%2Fa", "1")]),
    _xml(contents=[("revision%2Fz", "1"), ("revision%2Fa", "1")]),
    _xml(contents=[("foreign%2Fa", "1")]),
    _xml(contents=[("revision%2F..%2Fprivate", "1")]),
    _xml(contents=[("revision%2Fa%GG", "1")]),
    _xml(contents=[("revision%2Fa%FF", "1")]),
    _xml(contents=[("revision%2Fa", "-1")]),
    _xml(contents=[("revision%2Fa", "01")]),
    _xml(contents=[("revision%2Fa", "1.5")]),
    _xml(contents=[("revision%2Fa", "536870913")]),
    _xml(truncated="true"), _xml(truncated="TRUE"),
    _xml(fields="<NextContinuationToken>unexpected</NextContinuationToken>"),
    _xml(fields="<ContinuationToken>unexpected</ContinuationToken>"),
    _xml(fields="<Name>loom-bundles</Name>"),
    _xml().replace(b"<Size>7</Size>", b"<Size>7</Size><Size>7</Size>"),
    _xml().replace(b"<Key>", b"<Key unexpected='attribute'>", 1),
    _xml().replace(b"<Key>", b"<Key><Nested>", 1).replace(b"</Key>", b"</Nested></Key>", 1),
    _xml().replace(b"http://s3.amazonaws.com/doc/2006-03-01/", b"urn:foreign"),
    b"<!DOCTYPE x [<!ENTITY e 'expanded'>]>" + _xml(),
    b"<!DOCTYPE x SYSTEM 'file:///private'>" + _xml(),
    b"<?private instruction?>" + _xml(),
    _xml() + _xml(),
], ids=lambda _: "invalid-page")
def test_rejects_malformed_or_ambiguous_page_without_echo(payload):
    with pytest.raises(RuntimeError) as error:
        _parse(payload)
    assert "private" not in str(error.value)


def test_rejects_replayed_token_or_empty_truncated_progress():
    with pytest.raises(RuntimeError):
        _parse(_xml(truncated="true", fields="<ContinuationToken>same</ContinuationToken><NextContinuationToken>same</NextContinuationToken>"), continuation_token="same")
    with pytest.raises(RuntimeError):
        _parse(_xml(contents=[], truncated="true", fields="<NextContinuationToken>new</NextContinuationToken>"))


def test_rejects_excessive_page_bytes_before_xml_parser(monkeypatch):
    _module()

    def unexpected(*args, **kwargs):
        pytest.fail("oversized input must be refused before XML parser allocation")

    monkeypatch.setattr("xml.parsers.expat.ParserCreate", unexpected)
    with pytest.raises(RuntimeError):
        _parse(b"x" * (4 * 1024 * 1024 + 1))
