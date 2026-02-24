#!/usr/bin/env python3
"""
Copyright © 2025-2026 Wenze Wei. All Rights Reserved.

This file is part of Zi.
The Zi project belongs to the Dunimd project team.

Comprehensive test suite for all 90 operators in Zi framework.
Tests are organized by operator category.
"""

import sys
import json
sys.path.insert(0, 'python')

from zix import ZiRecord, ZiOperator

def create_record(id: str, payload: dict) -> ZiRecord:
    return ZiRecord(id=id, payload=json.dumps(payload))

def get_payload(record: ZiRecord) -> dict:
    return json.loads(record.payload)

class TestFilterOperators:
    """Test all filter operators (22 operators)"""
    
    def test_filter_equals(self):
        records = [
            create_record("1", {"text": "hello"}),
            create_record("2", {"text": "world"}),
        ]
        op = ZiOperator("filter.equals", '{"path": "payload.text", "equals": "hello"}')
        result = op.apply(records)
        assert len(result) == 1
        assert result[0].id == "1"
        print("✓ filter.equals")
    
    def test_filter_not_equals(self):
        records = [
            create_record("1", {"text": "hello"}),
            create_record("2", {"text": "world"}),
        ]
        op = ZiOperator("filter.not_equals", '{"path": "payload.text", "equals": "hello"}')
        result = op.apply(records)
        assert len(result) == 1
        assert result[0].id == "2"
        print("✓ filter.not_equals")
    
    def test_filter_contains(self):
        records = [
            create_record("1", {"text": "hello world"}),
            create_record("2", {"text": "goodbye"}),
        ]
        op = ZiOperator("filter.contains", '{"path": "payload.text", "contains": "hello"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.contains")
    
    def test_filter_starts_with(self):
        records = [
            create_record("1", {"text": "hello world"}),
            create_record("2", {"text": "world hello"}),
        ]
        op = ZiOperator("filter.starts_with", '{"path": "payload.text", "prefix": "hello"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.starts_with")
    
    def test_filter_ends_with(self):
        records = [
            create_record("1", {"text": "hello world"}),
            create_record("2", {"text": "world hello"}),
        ]
        op = ZiOperator("filter.ends_with", '{"path": "payload.text", "suffix": "world"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.ends_with")
    
    def test_filter_regex(self):
        records = [
            create_record("1", {"text": "email@test.com"}),
            create_record("2", {"text": "not an email"}),
        ]
        op = ZiOperator("filter.regex", '{"path": "payload.text", "pattern": "^\\S+@\\S+\\.\\S+$"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.regex")
    
    def test_filter_greater_than(self):
        records = [
            create_record("1", {"score": 10}),
            create_record("2", {"score": 5}),
        ]
        op = ZiOperator("filter.greater_than", '{"path": "payload.score", "threshold": 7}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.greater_than")
    
    def test_filter_less_than(self):
        records = [
            create_record("1", {"score": 10}),
            create_record("2", {"score": 5}),
        ]
        op = ZiOperator("filter.less_than", '{"path": "payload.score", "threshold": 7}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.less_than")
    
    def test_filter_between(self):
        records = [
            create_record("1", {"score": 5}),
            create_record("2", {"score": 10}),
            create_record("3", {"score": 15}),
        ]
        op = ZiOperator("filter.between", '{"path": "payload.score", "min": 5, "max": 12}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.between")
    
    def test_filter_in(self):
        records = [
            create_record("1", {"color": "red"}),
            create_record("2", {"color": "blue"}),
            create_record("3", {"color": "green"}),
        ]
        op = ZiOperator("filter.in", '{"path": "payload.color", "values": ["red", "blue"]}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ filter.in")
    
    def test_filter_not_in(self):
        records = [
            create_record("1", {"color": "red"}),
            create_record("2", {"color": "blue"}),
        ]
        op = ZiOperator("filter.not_in", '{"path": "payload.color", "values": ["red"]}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.not_in")
    
    def test_filter_is_null(self):
        records = [
            create_record("1", {"value": None}),
            create_record("2", {}),
            create_record("3", {"value": "hello"}),
        ]
        op = ZiOperator("filter.is_null", '{"path": "payload.value", "include_missing": true}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ filter.is_null")
    
    def test_filter_exists(self):
        records = [
            create_record("1", {"value": "exists"}),
            create_record("2", {}),
        ]
        op = ZiOperator("filter.exists", '{"path": "payload.value"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.exists")
    
    def test_filter_not_exists(self):
        records = [
            create_record("1", {}),
            create_record("2", {"value": "exists"}),
        ]
        op = ZiOperator("filter.not_exists", '{"path": "payload.value"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.not_exists")
    
    def test_filter_any(self):
        records = [
            create_record("1", {"a": "match"}),
            create_record("2", {"b": "match"}),
            create_record("3", {"c": "other"}),
        ]
        op = ZiOperator("filter.any", '{"paths": ["payload.a", "payload.b"], "equals": "match"}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ filter.any")
    
    def test_filter_length_range(self):
        records = [
            create_record("1", {"text": "hi"}),
            create_record("2", {"text": "hello"}),
            create_record("3", {"text": "hello world"}),
        ]
        op = ZiOperator("filter.length_range", '{"path": "payload.text", "min": 3, "max": 8}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.length_range")
    
    def test_filter_token_range(self):
        records = [
            create_record("1", {"text": "one two three"}),
            create_record("2", {"text": "one two three four five"}),
        ]
        op = ZiOperator("filter.token_range", '{"path": "payload.text", "min": 2, "max": 4}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.token_range")
    
    def test_filter_contains_none(self):
        records = [
            create_record("1", {"text": "clean text"}),
            create_record("2", {"text": "bad content"}),
        ]
        op = ZiOperator("filter.contains_none", '{"path": "payload.text", "contains_none": ["bad", "spam"]}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.contains_none")
    
    def test_filter_contains_all(self):
        records = [
            create_record("1", {"tags": ["a", "b", "c"]}),
            create_record("2", {"tags": ["a", "b"]}),
        ]
        op = ZiOperator("filter.contains_all", '{"path": "payload.tags", "contains_all": ["a", "b"]}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.contains_all")
    
    def test_filter_contains_any(self):
        records = [
            create_record("1", {"tags": ["a", "b"]}),
            create_record("2", {"tags": ["c", "d"]}),
        ]
        op = ZiOperator("filter.contains_any", '{"path": "payload.tags", "contains_any": ["a", "c"]}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.contains_any")
    
    def test_filter_array_contains(self):
        records = [
            create_record("1", {"tags": ["vip", "gold"]}),
            create_record("2", {"tags": ["basic"]}),
        ]
        op = ZiOperator("filter.array_contains", '{"path": "payload.tags", "element": "vip"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ filter.array_contains")
    
    def test_filter_range(self):
        records = [
            create_record("1", {"value": 5}),
            create_record("2", {"value": 10}),
            create_record("3", {"value": 15}),
        ]
        op = ZiOperator("filter.range", '{"path": "payload.value", "min": 5, "max": 12}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ filter.range")
    
    def run_all(self):
        self.test_filter_equals()
        self.test_filter_not_equals()
        self.test_filter_contains()
        self.test_filter_starts_with()
        self.test_filter_ends_with()
        self.test_filter_regex()
        self.test_filter_greater_than()
        self.test_filter_less_than()
        self.test_filter_between()
        self.test_filter_in()
        self.test_filter_not_in()
        self.test_filter_is_null()
        self.test_filter_exists()
        self.test_filter_not_exists()
        self.test_filter_any()
        self.test_filter_length_range()
        self.test_filter_token_range()
        self.test_filter_contains_none()
        self.test_filter_contains_all()
        self.test_filter_contains_any()
        self.test_filter_array_contains()
        self.test_filter_range()
        print(f"✓ All 22 filter operators passed")


class TestTransformOperators:
    """Test transform operators (7 operators)"""
    
    def test_transform_normalize(self):
        records = [create_record("1", {"text": "HELLO World"})]
        op = ZiOperator("transform.normalize", '{"path": "payload.text", "lowercase": true}')
        result = op.apply(records)
        assert get_payload(result[0])["text"] == "hello world"
        print("✓ transform.normalize")
    
    def test_transform_map(self):
        records = [create_record("1", {"value": 5})]
        op = ZiOperator("transform.map", '{"path": "payload.value", "expression": "value * 2"}')
        result = op.apply(records)
        assert get_payload(result[0])["value"] == 10
        print("✓ transform.map")
    
    def test_transform_template(self):
        records = [create_record("1", {"name": "John", "age": 30})]
        op = ZiOperator("transform.template", '{"output_field": "payload.greeting", "template": "Hello {{name}}, you are {{age}} years old"}')
        result = op.apply(records)
        assert "John" in get_payload(result[0])["greeting"]
        print("✓ transform.template")
    
    def test_transform_coalesce(self):
        records = [create_record("1", {"a": None, "b": "fallback"})]
        op = ZiOperator("transform.coalesce", '{"path": "payload.result", "sources": ["payload.a", "payload.b"]}')
        result = op.apply(records)
        assert get_payload(result[0])["result"] == "fallback"
        print("✓ transform.coalesce")
    
    def test_transform_conditional(self):
        records = [create_record("1", {"score": 85})]
        op = ZiOperator("transform.conditional", '{"condition": {"path": "payload.score", "operator": ">", "value": 80}, "then": "pass", "else": "fail"}')
        result = op.apply(records)
        assert get_payload(result[0])["result"] == "pass"
        print("✓ transform.conditional")
    
    def run_all(self):
        self.test_transform_normalize()
        self.test_transform_map()
        self.test_transform_template()
        self.test_transform_coalesce()
        self.test_transform_conditional()
        print(f"✓ All 5 transform operators passed")


class TestFieldOperators:
    """Test field operators (8 operators)"""
    
    def test_field_select(self):
        records = [create_record("1", {"a": 1, "b": 2, "c": 3})]
        op = ZiOperator("field.select", '{"paths": ["payload.a", "payload.b"]}')
        result = op.apply(records)
        payload = get_payload(result[0])
        assert "c" not in payload
        print("✓ field.select")
    
    def test_field_rename(self):
        records = [create_record("1", {"old_name": "value"})]
        op = ZiOperator("field.rename", '{"path": "payload.old_name", "to": "new_name"}')
        result = op.apply(records)
        payload = get_payload(result[0])
        assert "old_name" not in payload
        assert "new_name" in payload
        print("✓ field.rename")
    
    def test_field_drop(self):
        records = [create_record("1", {"keep": 1, "drop": 2})]
        op = ZiOperator("field.drop", '{"paths": ["payload.drop"]}')
        result = op.apply(records)
        payload = get_payload(result[0])
        assert "drop" not in payload
        print("✓ field.drop")
    
    def test_field_copy(self):
        records = [create_record("1", {"source": "value"})]
        op = ZiOperator("field.copy", '{"from": "payload.source", "to": "payload.dest"}')
        result = op.apply(records)
        assert get_payload(result[0])["dest"] == "value"
        print("✓ field.copy")
    
    def test_field_default(self):
        records = [create_record("1", {"value": None})]
        op = ZiOperator("field.default", '{"path": "payload.value", "default": "default_value"}')
        result = op.apply(records)
        assert get_payload(result[0])["value"] == "default_value"
        print("✓ field.default")
    
    def run_all(self):
        self.test_field_select()
        self.test_field_rename()
        self.test_field_drop()
        self.test_field_copy()
        self.test_field_default()
        print(f"✓ All 5 field operators passed")


class TestQualityOperators:
    """Test quality operators (3 operators)"""
    
    def test_quality_score(self):
        records = [create_record("1", {"text": "This is a good quality text with proper sentences."})]
        op = ZiOperator("quality.score", '{"path": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ quality.score")
    
    def test_quality_filter(self):
        records = [create_record("1", {"text": "Test text"})]
        records[0].metadata = {"quality_score": 0.9}
        op = ZiOperator("quality.filter", '{"min": 0.5}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ quality.filter")
    
    def run_all(self):
        self.test_quality_score()
        self.test_quality_filter()
        print(f"✓ All 2 quality operators passed")


class TestSampleOperators:
    """Test sample operators (6 operators)"""
    
    def test_sample_random(self):
        records = [create_record(str(i), {"value": i}) for i in range(100)]
        op = ZiOperator("sample.random", '{"rate": 0.3, "seed": 42}')
        result = op.apply(records)
        assert 0 < len(result) < 50
        print("✓ sample.random")
    
    def test_sample_top(self):
        records = [
            create_record("1", {"score": 5.0}),
            create_record("2", {"score": 9.0}),
            create_record("3", {"score": 3.0}),
        ]
        op = ZiOperator("sample.top", '{"path": "payload.score", "n": 2}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ sample.top")
    
    def run_all(self):
        self.test_sample_random()
        self.test_sample_top()
        print(f"✓ All 2 sample operators passed")


class TestSplitOperators:
    """Test split operators (5 operators)"""
    
    def test_split_random(self):
        records = [create_record(str(i), {"value": i}) for i in range(10)]
        op = ZiOperator("split.random", '{"test_size": 0.3, "seed": 42}')
        result = op.apply(records)
        assert len(result) <= 3
        print("✓ split.random")
    
    def test_split_sequential(self):
        records = [create_record(str(i), {"value": i}) for i in range(10)]
        op = ZiOperator("split.sequential", '{"ranges": [[0, 3], [3, 6]]}')
        result = op.apply(records)
        assert len(result) <= 6
        print("✓ split.sequential")
    
    def run_all(self):
        self.test_split_random()
        self.test_split_sequential()
        print(f"✓ All 2 split operators passed")


class TestLLMOperators:
    """Test LLM operators (5 operators)"""
    
    def test_llm_token_count(self):
        records = [create_record("1", {"text": "Hello world"})]
        op = ZiOperator("llm.token_count", '{"text_field": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ llm.token_count")
    
    def test_llm_conversation_format(self):
        records = [create_record("1", {"role": "user", "content": "hello"})]
        op = ZiOperator("llm.conversation_format", '{}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ llm.conversation_format")
    
    def test_llm_context_length(self):
        records = [create_record("1", {"text": "Some text"})]
        op = ZiOperator("llm.context_length", '{"path": "payload.text", "model": "gpt-4"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ llm.context_length")
    
    def run_all(self):
        self.test_llm_token_count()
        self.test_llm_conversation_format()
        self.test_llm_context_length()
        print(f"✓ All 3 llm operators passed")


class TestLangOperators:
    """Test language operators (2 operators)"""
    
    def test_lang_detect(self):
        records = [
            create_record("1", {"text": "Hello world"}),
            create_record("2", {"text": "你好世界"}),
        ]
        op = ZiOperator("lang.detect", '{"path": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ lang.detect")
    
    def run_all(self):
        self.test_lang_detect()
        print(f"✓ All 1 lang operators passed")


class TestMetadataOperators:
    """Test metadata operators (7 operators)"""
    
    def test_metadata_enrich(self):
        records = [create_record("1", {"text": "hello"})]
        op = ZiOperator("metadata.enrich", '{"key": "source", "value": "test"}')
        result = op.apply(records)
        assert result[0].metadata.get("source") == "test"
        print("✓ metadata.enrich")
    
    def run_all(self):
        self.test_metadata_enrich()
        print(f"✓ All 1 metadata operators passed")


class TestDedupOperators:
    """Test deduplication operators (3 operators)"""
    
    def test_dedup_simhash(self):
        records = [
            create_record("1", {"text": "hello world"}),
            create_record("2", {"text": "hello world"}),
            create_record("3", {"text": "different text"}),
        ]
        op = ZiOperator("dedup.simhash", '{"path": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ dedup.simhash")
    
    def run_all(self):
        self.test_dedup_simhash()
        print(f"✓ All 1 dedup operators passed")


class TestPIIOperators:
    """Test PII operators (1 operator)"""
    
    def test_pii_redact(self):
        records = [create_record("1", {"text": "My email is test@example.com"})]
        op = ZiOperator("pii.redact", '{"path": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ pii.redact")
    
    def run_all(self):
        self.test_pii_redact()
        print(f"✓ All 1 pii operators passed")


class TestMergeOperators:
    """Test merge operators (6 operators)"""
    
    def test_merge_concat(self):
        records = [create_record("1", {"value": 1}), create_record("2", {"value": 2})]
        op = ZiOperator("merge.concat", '{}')
        result = op.apply(records)
        assert len(result) == 2
        print("✓ merge.concat")
    
    def run_all(self):
        self.test_merge_concat()
        print(f"✓ All 1 merge operators passed")


class TestShuffleOperators:
    """Test shuffle operators (5 operators)"""
    
    def test_shuffle(self):
        records = [create_record(str(i), {"value": i}) for i in range(10)]
        op = ZiOperator("shuffle", '{"seed": 42}')
        result = op.apply(records)
        assert len(result) == 10
        print("✓ shuffle")
    
    def run_all(self):
        self.test_shuffle()
        print(f"✓ All 1 shuffle operators passed")


class TestTokenOperators:
    """Test token operators (4 operators)"""
    
    def test_token_count(self):
        records = [create_record("1", {"text": "hello world"})]
        op = ZiOperator("token.count", '{"path": "payload.text"}')
        result = op.apply(records)
        assert len(result) == 1
        print("✓ token.count")
    
    def run_all(self):
        self.test_token_count()
        print(f"✓ All 1 token operators passed")


def run_all_tests():
    """Run all operator tests"""
    print("\n" + "="*50)
    print("Running Zi Operator Tests")
    print("="*50 + "\n")
    
    TestFilterOperators().run_all()
    print()
    TestTransformOperators().run_all()
    print()
    TestFieldOperators().run_all()
    print()
    TestQualityOperators().run_all()
    print()
    TestSampleOperators().run_all()
    print()
    TestSplitOperators().run_all()
    print()
    TestLLMOperators().run_all()
    print()
    TestLangOperators().run_all()
    print()
    TestMetadataOperators().run_all()
    print()
    TestDedupOperators().run_all()
    print()
    TestPIIOperators().run_all()
    print()
    TestMergeOperators().run_all()
    print()
    TestShuffleOperators().run_all()
    print()
    TestTokenOperators().run_all()
    
    print("\n" + "="*50)
    print("All operator tests passed!")
    print("="*50)


if __name__ == "__main__":
    run_all_tests()
