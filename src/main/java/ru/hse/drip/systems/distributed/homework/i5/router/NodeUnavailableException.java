package ru.hse.drip.systems.distributed.homework.i5.router;

public class NodeUnavailableException extends RuntimeException {
    public NodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NodeUnavailableException(String message) {
        super(message);
    }
}
