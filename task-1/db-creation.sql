CREATE TABLE transactions_v2 (
    id Serial
    ,step Int32
    ,type Text
    ,amount Double
    ,nameOrig Text
    ,oldbalanceOrig Double
    ,newbalanceOrig Double
    ,isFraud Int8
    ,isFlaggedFraud Int8
    ,PRIMARY KEY (id)
);