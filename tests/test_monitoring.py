"""Tests for website structure monitoring functionality."""
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.monitor_idaho_structure import check_page_structure

@pytest.fixture
def valid_html():
    """Sample valid HTML structure."""
    return """
    <html>
        <body>
            <h3>Committee One</h3>
            <ul>
                <li>Member 1</li>
                <li>Member 2</li>
            </ul>
            <h3>Committee Two</h3>
            <ul>
                <li>Member 3</li>
                <li>Member 4</li>
            </ul>
            <h3>Committee Three</h3>
            <p>Chair: Member 5</p>
            <p>Vice Chair: Member 6</p>
            <h3>Committee Four</h3>
            <ol>
                <li>Member 7</li>
                <li>Member 8</li>
            </ol>
            <h3>Committee Five</h3>
            <ul>
                <li>Member 9</li>
                <li>Member 10</li>
            </ul>
        </body>
    </html>
    """

@pytest.fixture
def invalid_html():
    """Sample invalid HTML structure."""
    return """
    <html>
        <body>
            <h3>Single Committee</h3>
            <div>Some text without proper member list</div>
        </body>
    </html>
    """

@pytest.fixture
def empty_html():
    """Empty HTML structure."""
    return "<html><body></body></html>"

def test_check_page_structure_valid(valid_html):
    """Test structure checking with valid HTML."""
    with patch('src.monitor_idaho_structure.fetch_page', return_value=valid_html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is True

def test_check_page_structure_invalid(invalid_html):
    """Test structure checking with invalid HTML."""
    with patch('src.monitor_idaho_structure.fetch_page', return_value=invalid_html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is False

def test_check_page_structure_empty(empty_html):
    """Test structure checking with empty HTML."""
    with patch('src.monitor_idaho_structure.fetch_page', return_value=empty_html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is False

def test_check_page_structure_fetch_failure():
    """Test handling of fetch failures."""
    with patch('src.monitor_idaho_structure.fetch_page', return_value=None):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is False

def test_check_page_structure_malformed():
    """Test handling of malformed HTML."""
    malformed_html = "<html><body><h3>Unclosed Tag"
    with patch('src.monitor_idaho_structure.fetch_page', return_value=malformed_html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is False

def test_check_page_structure_content_after_heading(valid_html):
    """Test verification of content appearing after headings."""
    with patch('src.monitor_idaho_structure.fetch_page', return_value=valid_html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is True

def test_check_page_structure_minimum_headings():
    """Test minimum headings requirement."""
    html = """
    <html>
        <body>
            <h3>Committee One</h3>
            <ul><li>Member 1</li></ul>
            <h3>Committee Two</h3>
            <ul><li>Member 2</li></ul>
        </body>
    </html>
    """
    with patch('src.monitor_idaho_structure.fetch_page', return_value=html):
        result = check_page_structure('Test Page', 'http://example.com')
        assert result is False  # Should fail due to insufficient committees 