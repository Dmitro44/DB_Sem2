#import "stp/stp2024.typ"
#show: stp2024.template

#include("lab_title.typ")

#pagebreak()

= Индивидуальное задание

+ Создайте таблицу MyTable(id number, val number).
+ Напишите анонимный блок, который записывает в таблицу MyTable 10 000 целых случайных записей.
+ Напишите собственную функцию, которая выводит TRUE если четных значений val в таблице MyTable больше, FALSE если больше нечетных значений и EQUAL, если количество четных и нечетных равно.
+ Напишите функцию, которая по введенному значению ID, сгенерирует и выведет в консоль текстовое значение команды insert для вставки указанной строки.
+ Написать процедуры, реализующие DML операции (INSERT, UPDATE, DELETE) для указанной таблицы.
+ Создайте функцию, вычисляющую общее вознаграждение за год. На вход функции подаются значение месячной зарплаты и процент годовых премиальных. В общем случае общее вознаграждение= (1+ процент годовых премиальных)\*12\* значение месячной зарплаты. При этом предусмотреть что процент вводится как целое число, и требуется преобразовать его к дробному. Предусмотреть защиту от ввода некорректных данных.

= Краткие теоретические сведения

PL/SQL — это процедурное расширение языка SQL, предназначенное для разработки программной логики, выполняемой на стороне сервера базы данных Oracle. Использование PL/SQL позволяет объединять SQL-запросы и управляющие конструкции в одном программном блоке, что упрощает работу с данными и повышает эффективность их обработки. В рамках первой лабораторной работы PL/SQL применяется для создания простых подпрограмм и выполнения базовых операций над таблицами.

Основной структурной единицей PL/SQL является блок. Блок содержит исполняемую часть, в которой размещаются SQL-операторы и процедурные конструкции, а также может включать раздел объявлений переменных. Такая структура позволяет хранить промежуточные данные в переменных и использовать их при выполнении операций вставки, обновления и анализа данных в таблицах.

В PL/SQL поддерживаются различные типы данных, необходимые для хранения числовых и логических значений. В первой лабораторной работе чаще всего используются числовые типы, такие как NUMBER, а также логический тип BOOLEAN. Для текстовых данных применяется тип VARCHAR2. Корректный выбор типа данных обеспечивает правильность вычислений и обработки информации при выполнении процедур и функций.

Процедуры в PL/SQL представляют собой подпрограммы, предназначенные для выполнения определённых действий. Они используются для реализации операций изменения данных, таких как вставка, обновление и удаление записей в таблице. Процедуры позволяют инкапсулировать логику работы с данными и вызывать её многократно без дублирования кода.

Функции отличаются от процедур тем, что обязательно возвращают значение. В первой лабораторной работе функции применяются для анализа данных, например для сравнения количества чётных и нечётных значений или для вычисления итоговых величин на основе входных параметров. Возвращаемое функцией значение может использоваться в логических выражениях и управляющих конструкциях.


= Выполнение работы

== Создание таблицы MyTable

Создана таблица MyTable с двумя полями: id (первичный ключ) и val (числовое значение). Эта таблица будет использоваться для хранения случайных числовых данных в последующих пунктах работы.

#stp2024.listing[Команда для создания таблицы MyTable][
  ```
  CREATE TABLE MyTable (
      id NUMBER PRIMARY KEY,
      val NUMBER
  );
  ```
]

== Вставка 10 000 случайных записей

Разработан анонимный PL/SQL блок, использующий массивы (коллекции) для эффективной вставки 10 000 записей с помощью оператора FORALL. Значения val генерируются случайным образом в диапазоне от 0 до 500 000 с использованием функции DBMS_RANDOM.VALUE.

#stp2024.listing[Анонимный блок для вставки 10 000 записей][
  ```
  DECLARE
      TYPE t_ids IS TABLE OF NUMBER;
      TYPE t_vals IS TABLE OF NUMBER;

      v_ids t_ids := t_ids();
      v_vals t_vals := t_vals();
  BEGIN
      FOR i IN 1..10000 LOOP
          v_ids.extend;
          v_vals.extend;

          v_ids(i) := i;
          v_vals(i) := ROUND(DBMS_RANDOM.VALUE(0, 500000));
      end loop;

      FORALL i IN 1..v_ids.COUNT
          INSERT INTO MYTABLE (id, val)
          VALUES (v_ids(i), v_vals(i));

      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Inserted ' || SQL%ROWCOUNT || ' entries');
  EXCEPTION
      WHEN OTHERS THEN
          ROLLBACK;
          DBMS_OUTPUT.PUT_LINE('Error ' || SQLERRM);
  end;
  ```
]

== Функция проверки четности значений

Создана функция check_odd_even, которая анализирует все значения val в таблице и возвращает 'TRUE', если четных значений больше, 'FALSE', если больше нечетных, и 'EQUAL', если их количество равно. Функция использует агрегатные функции и условные выражения CASE для подсчета.

#stp2024.listing[Функция проверки четности значений][
  ```
  CREATE OR REPLACE FUNCTION check_odd_even
  RETURN VARCHAR2
  IS
      v_even_count NUMBER;
      v_odd_count NUMBER;
  BEGIN
      SELECT
          SUM(CASE WHEN MOD(val, 2) = 0 THEN 1 ELSE 0 END),
          SUM(CASE WHEN MOD(val, 2) != 0 THEN 1 ELSE 0 END)
      INTO v_even_count, v_odd_count
      FROM MYTABLE;

      IF v_even_count > v_odd_count THEN
          RETURN 'TRUE';
      ELSIF v_odd_count > v_even_count THEN
          RETURN 'FALSE';
      ELSE
          RETURN 'EQUAL';
      END IF;
  end;
  ```
]

== Генерация команды INSERT

Реализована процедура gen_insert_cmd, которая по заданному ID находит соответствующую запись в таблице и формирует текстовую команду INSERT для её вставки. Команда выводится в консоль через DBMS_OUTPUT, предусмотрена обработка исключения при отсутствии записи с указанным ID.

#stp2024.listing[Процедура генерации команды INSERT][
  ```
  CREATE OR REPLACE PROCEDURE gen_insert_cmd(p_id IN MYTABLE.ID%type)
  IS
      v_val MYTABLE.VAL%type;
      v_insert_command VARCHAR2(500);
  BEGIN
      SELECT VAL
      INTO v_val
      FROM MYTABLE
      WHERE id = p_id;

      v_insert_command := 'INSERT INTO MYTABLE (id, val) VALUES (' ||
                          p_id || ', ' || v_val || ');';

      DBMS_OUTPUT.PUT_LINE(v_insert_command);

  EXCEPTION
      WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('Line with ID ' || p_id || ' not found');
      WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
  end;
  ```
]

== Процедуры DML операций

Разработаны три процедуры для выполнения операций с данными таблицы MyTable: insert_proc для вставки новых записей с проверкой дублирования ID, update_proc для изменения значения val по указанному ID, и delete_proc для удаления записи. Все процедуры включают обработку исключений и управление транзакциями.

#stp2024.listing[Процедура INSERT][
  ```
  CREATE OR REPLACE PROCEDURE insert_proc(
      p_id IN MYTABLE.ID%type,
      p_val IN MYTABLE.VAL%type
  )
  IS
  BEGIN
      INSERT INTO MYTABLE (id, val)
      VALUES (p_id, p_val);

      COMMIT;
  EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
          ROLLBACK;
          DBMS_OUTPUT.PUT_LINE('Line with ID ' || p_id ||
                              ' already exists');
      WHEN OTHERS THEN
          ROLLBACK;
          DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
  end insert_proc;
  ```
]

#stp2024.listing[Процедура INSERT][
  ```
  CREATE OR REPLACE PROCEDURE update_proc(
      p_id IN MYTABLE.ID%type,
      p_val IN MYTABLE.VAL%type
  )
  IS
  BEGIN
      UPDATE MYTABLE
      SET val = p_val
      WHERE ID = p_id;

      IF SQL%ROWCOUNT = 0 THEN
          DBMS_OUTPUT.PUT_LINE('Update was not completed. ID ' ||
                              p_id || ' not found');
      ELSE
          COMMIT;
      end if;
  EXCEPTION
      WHEN OTHERS THEN
          ROLLBACK;
          DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
  end update_proc;
  ```
]

#stp2024.listing[Процедура DELETE][
  ```
  CREATE OR REPLACE PROCEDURE delete_proc(p_id IN MYTABLE.ID%type)
  IS
  BEGIN
      DELETE FROM MYTABLE
      WHERE ID = p_id;

      IF SQL%ROWCOUNT = 0 THEN
          DBMS_OUTPUT.PUT_LINE('Delete was not completed. ID ' ||
                              p_id || ' not found');
      ELSE
          COMMIT;
      end if;
  EXCEPTION
      WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
  end delete_proc;
  ```
]


== Функция расчета годового вознаграждения

Создана функция calc_award для вычисления общего годового вознаграждения по формуле: (1 + процент/100) × 12 × месячная_зарплата. Реализована валидация входных данных: проверка целочисленности процента, его диапазона (0-100) и положительности месячной зарплаты с выбросом соответствующих исключений при нарушении условий.

#stp2024.listing[Функция расчета годового вознаграждения][
  ```
  CREATE OR REPLACE FUNCTION calc_award(
      month_salary NUMBER,
      bonus_perc NUMBER
  )
  RETURN NUMBER
  IS
      v_bonus_float NUMBER := bonus_perc / 100;
      v_result NUMBER;
  BEGIN
      if bonus_perc != TRUNC(bonus_perc) THEN
          RAISE_APPLICATION_ERROR(-20003,
              'Error: Bonus percentage (' || bonus_perc ||
              ') should be integer');
      end if;

      if bonus_perc < 0 OR bonus_perc > 100 THEN
          RAISE_APPLICATION_ERROR(-20001,
              'Error: Bonus percentage (' || bonus_perc ||
              ') should be in range from 0 to 100');
      end if;

      if month_salary IS NULL OR month_salary <= 0 THEN
          RAISE_APPLICATION_ERROR(-20002,
              'Error: Monthly salary (' || month_salary ||
              ') should be positive number');
      end if;

      v_result := (1 + v_bonus_float) * 12 * month_salary;

      RETURN v_result;
  end calc_award;
  ```
]

#stp2024.heading_unnumbered[Вывод]

В ходе выполнения лабораторной работы были получены практические навыки разработки на языке PL/SQL в среде Oracle. Были освоены основные конструкции языка: анонимные блоки, процедуры и функции. Реализованы механизмы работы с данными, включая массовую вставку записей с использованием коллекций и оператора FORALL для повышения производительности. Также была изучена обработка исключений и валидация входных параметров для обеспечения надежности программной логики. В результате работы закреплены знания о взаимодействии процедурного кода с реляционными таблицами и методах автоматизации DML-операций.

