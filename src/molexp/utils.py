import hashlib
import inspect
import string
from typing import Any


def validate_string_input(user_input):
    """Validate the experiment name will make a valid directory name"""
    allowed = set(string.ascii_letters + string.digits + "_" + "-" + ".")
    for char in user_input:
        if char not in allowed:
            raise ValueError(f"`{char}` from `{user_input}` is an invalid character.")


def _get_default_input(node) -> Any:
    """Get default input node value from originating function signature"""
    param_name = node.name
    origin_function = node.originating_functions[0]
    param = inspect.signature(origin_function).parameters[param_name]
    return None if param.default is inspect._empty else param.default


def json_encoder(obj: Any) -> list:
    """convert non JSON-serializable objects to a serializable format

    set[T] -> list[T]
    else -> dict[type: str, byte_hash: str]
    """
    if isinstance(obj, set):
        serialized = list(obj)
    else:
        obj_hash = hashlib.sha256()
        obj_hash.update(obj)
        serialized = dict(
            dtype=type(obj).__name__,
            obj_hash=obj_hash.hexdigest(),
        )
    return serialized
