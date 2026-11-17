package org.lineagedq.spark.dq;

/**
 * Nem ra khi mot checkpoint quyet dinh HALT va DQ_GATE_ENFORCE dang bat.
 *
 * Day la NGOAI LE DUY NHAT cua nguyen tac "DQ khong bao giu lam hong pipeline". Ly do: mot gate
 * co chu dich chan job LAI som de khoi dot compute cho du lieu da hong - neu nuot loi thi gate
 * khong con la gate. Moi loi KHAC trong DQ van bi nuot nhu cu.
 *
 * Tat bang DQ_GATE_ENFORCE=false: luc do HALT chi con la canh bao trong log va trong report.
 */
public class DqHaltException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String stage;

    public DqHaltException(String stage, String reason) {
        super("DQ gate '" + stage + "' quyet dinh HALT: " + reason);
        this.stage = stage;
    }

    public String stage() {
        return stage;
    }
}
