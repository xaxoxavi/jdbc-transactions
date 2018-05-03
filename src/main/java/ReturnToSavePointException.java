/**
 * Created by xavi on 3/05/18.
 * L'objectiu d'aquesta excepció és controlar el posible
 * retorn a un determinat checkpoint
 */
public class ReturnToSavePointException extends Exception {

    public ReturnToSavePointException(){

    }

    public ReturnToSavePointException(String message) {
        super(message);
    }
}
