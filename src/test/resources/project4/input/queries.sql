SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100;
SELECT DISTINCT S.A, R.G FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 ORDER BY S.A;
SELECT R.G, S.B FROM Reserves R, Sailors S, Boats B, Teams T WHERE R.G <> B.D AND R.G = S.B AND S.C = T.I AND R.G = 2 AND T.I = T.K AND B.E <> 42;