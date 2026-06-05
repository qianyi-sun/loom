import pytest
from pydantic import ValidationError

from loom.models.mcp import MCPConnection


def test_stdio_requires_command():
    with pytest.raises(ValidationError, match="stdio transport requires `command`"):
        MCPConnection(name="x", transport="stdio")


def test_stdio_rejects_url():
    with pytest.raises(ValidationError, match="stdio transport must not set `url`"):
        MCPConnection(
            name="x", transport="stdio", command=["mcp-server"], url="http://x",
        )


def test_stdio_happy_path():
    conn = MCPConnection(
        name="memory", transport="stdio", command=["memory-mcp"],
        env={"DEBUG": "1"},
    )
    assert conn.command == ["memory-mcp"]
    assert conn.url is None


@pytest.mark.parametrize("transport", ["sse", "websocket", "http"])
def test_remote_requires_url(transport):
    with pytest.raises(ValidationError, match=f"{transport} transport requires `url`"):
        MCPConnection(name="x", transport=transport)


@pytest.mark.parametrize("transport", ["sse", "websocket", "http"])
def test_remote_rejects_command(transport):
    with pytest.raises(ValidationError, match=f"{transport} transport must not set `command`"):
        MCPConnection(
            name="x", transport=transport, url="http://x", command=["cmd"],
        )


def test_http_with_headers():
    conn = MCPConnection(
        name="search", transport="http", url="https://api/search",
        headers={"Authorization": "Bearer token"},
    )
    assert conn.url == "https://api/search"
    assert conn.headers == {"Authorization": "Bearer token"}
