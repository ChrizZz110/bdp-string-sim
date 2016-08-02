package org.bdp.string_sim.exception;

public class NotFoundInDictionaryException extends Exception {
    private final String nGram;

    public NotFoundInDictionaryException(String nGram) {
        super();
        this.nGram = nGram;
    }
}
