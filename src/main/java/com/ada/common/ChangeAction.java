package com.ada.common;

public interface ChangeAction<FROM, TO> {
    TO action(FROM from);
}
