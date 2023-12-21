REPLACE PROCEDURE dummy_test_tmode.CurrencyConversionProcedureANSI(
    IN p_amount DECIMAL(18, 2),
    IN p_original_currency VARCHAR(3),
    IN p_target_currency VARCHAR(3)
)
BEGIN
    DECLARE v_exchange_rate DECIMAL(18, 6);
    SET v_exchange_rate = 1.2;
    INSERT INTO currency_table (
        amount,
        original_currency,
        converted_amount
    ) VALUES (
        p_amount,
        p_original_currency,
        p_amount * v_exchange_rate
    );
END;
