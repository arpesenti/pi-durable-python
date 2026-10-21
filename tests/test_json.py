import json
import math

import pytest

from pi_durable.json import (
    FrozenDict,
    FrozenList,
    copy_json,
    freeze_json,
    is_json_value,
)


def test_accepts_every_strict_json_shape() -> None:
    value: dict[str, object] = {
        "null": None,
        "true": True,
        "false": False,
        "int": 1,
        "float": 1.5,
        "string": "text",
        "array": [1, "two", None, {"nested": []}],
        "object": {"empty": {}},
    }
    assert is_json_value(value) is True
    assert copy_json(value) == value
    assert is_json_value(json.loads(json.dumps(value))) is True


def test_rejects_non_json_values() -> None:
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    for invalid in [
        {"omitted": object()},
        {"undefined": ...},
        {1: "non-string key"},
        b"bytes",
        (1, 2),
        {1, 2},
        math.inf,
        -math.inf,
        math.nan,
        cyclic,
    ]:
        assert is_json_value(invalid) is False
        with pytest.raises(TypeError, match="strict JSON"):
            copy_json(invalid)


def test_rejects_container_subclasses_that_are_not_json_values() -> None:
    class DictSubclass(dict[str, object]):
        pass

    class ListSubclass(list[object]):
        pass

    for invalid in [DictSubclass(value=1), ListSubclass([1])]:
        assert is_json_value(invalid) is False
        with pytest.raises(TypeError, match="strict JSON"):
            copy_json(invalid)


def test_copies_without_retaining_aliases() -> None:
    shared = {"value": 1}
    input_value = {"left": shared, "right": shared}
    copied = copy_json(input_value)
    assert isinstance(copied, dict)
    assert copied == input_value
    assert copied is not input_value
    assert isinstance(copied["left"], dict)
    assert isinstance(copied["right"], dict)
    assert copied["left"] is not shared
    assert copied["right"] is not shared
    assert copied["left"] is not copied["right"]

    array = [shared]
    copied_array = copy_json([array, array])
    assert isinstance(copied_array, list)
    assert isinstance(copied_array[0], list)
    assert isinstance(copied_array[1], list)
    assert isinstance(copied_array[0][0], dict)
    assert copied_array[0] is not copied_array[1]
    assert copied_array[0][0] is not shared


def test_frozen_values_are_immutable_json_containers() -> None:
    frozen = freeze_json({"data": {"items": [1, {"deep": True}]}})
    assert isinstance(frozen, FrozenDict)
    assert isinstance(frozen["data"], FrozenDict)
    assert isinstance(frozen["data"]["items"], FrozenList)
    assert json.dumps(frozen) == '{"data": {"items": [1, {"deep": true}]}}'
    assert json.loads(json.dumps(frozen)) == frozen

    with pytest.raises(TypeError, match="immutable"):
        frozen["data"] = {}
    with pytest.raises(TypeError, match="immutable"):
        frozen["data"]["items"].append(2)
    with pytest.raises(TypeError, match="immutable"):
        frozen["data"]["items"][1]["deep"] = False
    with pytest.raises(TypeError, match="immutable"):
        frozen["data"]["items"][1].update({"other": 1})


def test_freeze_validates_and_detaches() -> None:
    source = {"items": [1]}
    frozen = freeze_json(source)
    source["items"].append(2)
    assert frozen == {"items": [1]}
    with pytest.raises(TypeError, match="strict JSON"):
        freeze_json({"bad": object()})


def test_accepts_null_prototype_like_keys_verbatim() -> None:
    copied = copy_json({"__proto__": {"safe": True}, "constructor": "value"})
    assert copied == {"__proto__": {"safe": True}, "constructor": "value"}
