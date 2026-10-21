import json

import pytest

from pi_durable.ids import (
    ROOT_CONVERSATION_ID,
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
    id_from_number,
    seq_from_number,
)


def test_ids_are_ordinary_numbers_that_round_trip_through_json() -> None:
    conversation_id: ConversationId = id_from_number(7)
    entry_id: EntryId = id_from_number(8)
    task_id: TaskId = id_from_number(9)
    submission_id: SubmissionId = id_from_number(10)
    document_id: DocumentId = id_from_number(11)

    assert isinstance(conversation_id, int)
    assert json.dumps(conversation_id) == "7"
    assert [entry_id, task_id, submission_id, document_id] == [8, 9, 10, 11]
    assert json.loads(json.dumps({"id": int(document_id)})) == {"id": 11}


def test_root_conversation_id_is_reserved() -> None:
    assert ROOT_CONVERSATION_ID == 1
    assert isinstance(ROOT_CONVERSATION_ID, int)


def test_sequences_are_ordinary_numbers_that_round_trip_through_json() -> None:
    sequence = seq_from_number(3)
    assert isinstance(sequence, int)
    assert json.dumps(sequence) == "3"
    assert json.loads(json.dumps(sequence)) == 3


@pytest.mark.parametrize("value", [0, -1, 1.0, "2", True, False, None])
def test_id_boundary_rejects_values_that_are_not_positive_integers(value: object) -> None:
    with pytest.raises(TypeError):
        id_from_number(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1, 1.0, "2", True, False, None])
def test_sequence_boundary_rejects_values_that_are_not_positive_integers(value: object) -> None:
    with pytest.raises(TypeError):
        seq_from_number(value)  # type: ignore[arg-type]


def test_new_type_brands_have_no_runtime_wrapper() -> None:
    assert ConversationId(1) == 1
    assert EntryId(2) == 2
    assert TaskId(3) == 3
    assert SubmissionId(4) == 4
    assert DocumentId(5) == 5
    assert Seq(6) == 6
