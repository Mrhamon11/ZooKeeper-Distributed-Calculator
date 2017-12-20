package org.apache.zookeeper.book;

import java.math.BigDecimal;

/**
 * Helps determine if the task passed in meets the required format for the system
 * to work. Contains one public method, checkRequestValid.
 */
public class RequestValidator {
    private String[] request;

    public RequestValidator(String request){
        this.request = request.trim().toLowerCase().split(" ");
    }

    /**
     * Returns true if the request passed in meets the format required by the system
     * false otherwise.
     * @return
     */
    public boolean checkRequestValid(){
        boolean requestValid;

        if(this.request[0].equals("insert")){
            requestValid = checkValidInsert();
        }
        else if(this.request[0].equals("retrieve") || this.request[0].equals("delete"))
            requestValid = checkValidRetrieveAndDelete();
        else if(this.request[0].equals("submit"))
            requestValid = checkValidSubmit();
        else{
            requestValid = false;
        }

        return requestValid;
    }


    private boolean checkValidInsert(){
        return request.length == 3 && isDecimal(request[2]);
    }

    private boolean checkValidRetrieveAndDelete(){
        return request.length == 2;
    }

    private boolean checkValidSubmit(){
        return request.length >= 4 && isOperator(request[1]);
    }

    private boolean isDecimal(String num){
        try{
            BigDecimal d = new BigDecimal(num);
            return true;
        }catch (NumberFormatException e){
            return false;
        }
    }

    private boolean isOperator(String op){
        return op.equals("+") || op.equals("-") || op.equals("*") || op.equals("/");
    }
}
