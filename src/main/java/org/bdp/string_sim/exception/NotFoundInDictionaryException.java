package org.bdp.string_sim.exception;

public class NotFoundInDictionaryException extends Exception {
    private String nGram;

    public NotFoundInDictionaryException(String nGram) {
        super();
        this.nGram = nGram;
    }

    public String getnGram() {
        return nGram;
    }
}
