# Интерфейсно-Объектная библиотечка fobj - funny objects.

Библиотечка призвана предоставить решение проблеме полиформизма:
- иметь множество реализаций одного поведения (методов),
- и прозрачное инициирование поведения не зависимо от реализации (вызов
  метода).

Реализует концепцию динамического связывания:
- объект аллоцируется со скрытым хедером, содержащим идентификатор класса
- во время вызова метода, реализация метода ищется в рантайме используя
  идентификатор класса из хедера объекта и идентификатор метода.

Плюс, библиотека предоставляет управление временем жизни объектов
посредством счётчика ссылок.

### Пример

Рассмотрим на примере создания объекта с состоянием в виде переменной
double и паре методов: "умножить и прибавить" и "прибавить и умножить".

Без "магии" это могло выглядеть так:
```c
    typedef struct dstate { double st; } dstate;
    double mult_and_add(dstate *st, double mult, double add) {
        st->st = st->st * mult + add;
        return st->st;
    }

    double add_and_mult(dstate *st, double add, double mult) {
        st->st = (st->st + add) * mult;
        return st->st;
    }

    int main (void) {
        dstate mystate = { 1 };
        printf("%f\n", mult_and_add(&mystate, 2, 1));
        printf("%f\n", add_and_mult(&mystate, 2, 1));
        printf("%f\n", mult_and_add(&mystate, 5, 8));
        printf("%f\n", add_and_mult(&mystate, 5, 8));
    }
```

## Метод

Метод - главный персонаж библиотеки. Определяет поведение.

Метод - "дженерик" функция с диспатчингом по первому аргументу.
Первый аргумент не типизирован. Им может быть "произвольный" объект.
Типы второго и следующего аргумента фиксированные.

Аргуметны могут быть "обязательными" и "опциональными". У опционального
аргумента может быть значение по умолчанию.

Диспатчинг происходит в рантайме. Если обнаружится, что на объекте
метод не определён, произойдёт FATAL ошибка с абортом. Если обязательный
аргумент не передан, тоже.

Имена методов строго уникальны, потому именовать метод рекомендуется
с абревиатурой неймспейса. Также рекомендуется использовать
theCammelCase.

Чтобы создать метод, нужно:

- объявить имя, сигнатуру метода.
  Обычно это делается в заголовочных файлах (.h)
  Первый аргумент (объект) явно объявлять не нужно.
```c
    #define mth__hotMultAndAdd double, (double, mult), (double, add)
    #define mth__hotAddAndMult double, (double, add), (double, mult)
    #define mth__hotGetState   double
```
- если есть опциональные аргументы, то добавить их объявление.
  (если нет, то макрос объявлять не нужно)
```c
    #define mth__hotMultAndAdd_optional() (mult, 1), (add, 0)
```
- позвать генерирующий макрос (так же в .h файле, если это метод должен
  быть виден из-вне)
```c
    fobj_method(hotMultAndAdd);
    fobj_method(hotAddAndMult);
    fobj_method(hotGetState);
```

  Макрос генерит следующие объявления, используемые пользователем:

```c
    // Функция вызова метода без "магии" именованных/опциональных
    // параметров.
    static inline double    hotMultAndAdd(fobj_t obj, double mult, double add);

    // Интерфейс для одного метода
    typedef union hotMultAndAdd_i {
        fobj_t      self;
        uintptr_t   has_hotMultAndAdd;
    } hotMultAndAdd_i;

    // Биндинг интерфейса для объекта
    static inline hotMultAndAdd_i   bind_hotMultAndAdd(fobj_t obj);

    // Биндинг интерфейса + увеличение счётчика ссылок на объекте.
    static inline hotMultAndAdd_i   bindref_hotAddAndMult(fobj_t obj);

    // Проверка реализации интерфейса
    static inline bool  implements_hotAddAndMult(fobj_t obj, hotMultAndAdd_i *iface);
```

  Последующие объявления пользователем непосредственно не используются.
  Можете не читать.

```c
    // Хэндл метода
    static inline fobj_method_handle_t     hotMultAndAdd__mh(void);

    // Тип функции - реализации
    typedef double (*hotMultAndAdd__impl)(fobj_t obj, double mult, double mult);

    // Связанная реализация
    typedef struct hotMultAndAdd_cb {
        fobj_t              self;
        hotMultAndAdd_impl  impl;
    } hotMultAndAdd_cb;

    // Получение связанной реализации
    static inline hotMultAndAdd_cb
    fobj_fetch_hotMultAndAdd(fobj_t obj, fobj_klass_handle_t parent);

    // Регистрация реализации
    static inline void
    fobj__register_hotMultAndAdd(fobj_klass_handle_t klass, hotMultAndAdd_impl impl);

    // Валидация существования метода для класса
    // (В случае ошибки, будет FATAL)
    static inline void
    fobj__klass_validate_hotMultAndAdd(fobj_klass_handle_t klass);

    // Тип для именованных параметров
    typedef struct hotMultAndAdd__params_t {
        fobj__dumb_t    _dumb_first_param;

        double          mult;
        fobj__dumb_t    mult__given;

        double          add;
        fobj__dumb_t    add__given;
    } hotMultAndAdd__params_t;

    // Функция вызова связанной реализации с параметрами.
    static inline double
    fobj__invoke_hotMultAndAdd(hotMultAndAdd__cb cb, hotMultAndAdd__params params);
```

------

## Класс

Класс определяет связывание методов с конкретными объектами.

Объявляя класс, можно указать:
- список методов
- родителя (опционально)
- интерфейсы, которые нужно провалидировать (опционально)
- расширяемая ли аллокация (опционально)

Чтобы создать класс, нужно:
- объявить тело класса
```c
    typedef struct hotDState {
        double st;
    } hotDState;

    // И "наследника"
    typedef struct hotCState {
        hotDState p;        // parent
        double    add_acc;  // аккумулятор для слагаемых
    } hotCState;
```

- обявить сигнатуру класса
```c
    #define kls__hotDState  mth(hotMultAndAdd, hotGetState), \
                            iface(hotState)
    #define kls__hotCState  inherits(hotDState), \
                            mth(hotMultAndAdd, hotAddAndMult),
                            iface(hotState)
```
  Примечания:
  - мы не объявили hotAddAndMult на hotDState, но он удовлетворяет
    hotState, т.к. hotAddAndMult не обязателен.
  - мы не объявляли hotGetState на hotCState, но он удовлетворяет
    hotState, т.к. этот метод он наследует от hotDState.

- позвать генерирующие макросы
```c
    fobj_klass(hotDState);
    fobj_klass(hotCState);
```
  На самом деле, это всего лишь генерит методы хэндл-ов.
```c
    extern fobj_khandle_method_t    hotDState__kh(void);
    extern fobj_khandle_method_t    hotCState__kh(void);
```

- объявить реализации методов:
```c
    static double
    hotDState_hotMultAndAdd(VSelf, double mult, double add) {
        Self(hotDState);
        self->st = self->st * mult + add;
        return self->st;
    }

    static double
    hotDState_hotGetState(VSelf) {
        Self(hotDState);
        return self->st;
    }

    static double
    hotCState_hotMultAndAdd(VSelf, double mult, double add) {
        Self(hotCState);
        // Вызов метода на родителе
        $super(hotMultAndAdd, self, .mult = mult, .add = add);
        self->add_acc += add;
        return self->st;
    }

    static double
    hotCState_hotAddAndMult(VSelf, double add, double mult) {
        Self(hotCState);
        $(hotMultAndAdd, self, .add = add);
        $(hotMultAndAdd, self, .mult = mult);
        return self->st;
    }
```

- После всех реализаций (или хотя бы их прототипов) в одном .c файле нужно создать реализацию хэндла класса:
```c
    fobj_klass_handle(hotDState);
    fobj_klass_handle(hotCState);
```

- Опционально, можно инициализировать класс.
  Этот шаг не обязателен чаще всего, но может потребоваться, если вы собираетесь
  заморозить рантайм (`fobj_freeze`), или захотите поискать класс по имени.
```c
    void
    libarary_initialization(void) {
        /*...*/
        fobj_klass_init(hotDState);
        fobj_klass_init(hotCState);
        /*...*/
    }
```

### Деструктор

Когда объект уничтожается, выполняется стандартный метод `fobjDispose`
(если определён на объекте).
```c
    typedef struct myStore {
        fobj_t field;
    } myStore;

    #define kls__myKlass mth(fobjDispose)

    static void
    myKlass_fobjDispose(VSelf) {
        Self(myKlass);
        $del(&self->field);
    }
```

Методы `fobjDispose` вызываются для всех классов в цепочке наследования,
для которых они определены, по порядку *от потомков к первому родителю*.
Т.е. сперва вызвается `fobjDispose` вашего класса, потом его
непосредственного родителя, потом родителя родителя и т.д.

Нет способа вернуть ошибку из `fobjDispose` (т.к. не куда). Он обязан
сам со всем разобраться.

Иногда нужно "отключить объект" не дожидаясь, пока он сам уничтожится.
Т.е. хочется позвать `fobjDispose`. Но явный вызов `fobjDispose` запрещён.
Для этого нужно воспользоваться обёрткой `fobj_dispose`.

Обёртка гарантирует, что `fobjDispose` будет позван только один раз.
Кроме того, она запоминает, что вызов `fobjDispose` завершился, и после
этого любой вызов метода (вызванный без хаков) на этом объекте будет
падать в FATAL.

### Методы класса

Долго думал, и решил, что нет проблем, решаемых методами класса,
и не решаемых другими методами.

Методы класса играют обычно роли:
- неймспейса для статических функций.
  - Но в С можно просто звать глобальные функции.
- синглтон объектов, связанных с множеством объектов.
  - Если объекту очень нужен синглтон, можно объявить метод,
    возвращающий такой синглтон. Но большинство объектов не требует
    связанного с ним синглтона.
- фабрик для создания объектов
  - Что не отличается от кейса со статическими функциями.

В общем, пока не появится очевидная необходимость в методах класса,
делать их не буду. Ибо тогда потребуется создавать мета-классы и их
иерархию. Что будет серьёзным усложнением рантайма.

### Конструктор

И тут вероятно вы зададите мне вопрос:
- Эй, уважаемый, что за омлет? А где же яйца?
    (с) Дискотека Авария.

В смысле: до сих пор ни слова про конструкторы.

И не будет. Применяется подход "Конструктор бедного человека":
- конструированием объекта должен заниматься не объект.
- объект конструирует либо глобальная функция, либо метод другого объекта.
  - либо всё нужное для работы объекта готовится перед аллокацией и
    передаётся непосредственно в значения полей в момен аллокации,
  - либо у объекта есть некий метод `initializeThisFu__ingObject`
    который зовётся после аллокации.
    (название выдуманно, такого стандартного метода нет),

Этот подход применён в Go, и в целом, его можно понять и принять.
Сделать семантику конструктора с корректной обработкой возможных ошибок,
вызова родительского конструктора и прочим не просто. И не сказать,
чтобы языки, имеющие конструкторы, справляются с ними без проблем.

В библиотечке так же наложились проблемы:
- с сохранением где-нибудь имени метода(?) или функции(?) конструктора
  и передачи ему параметров.
- перегрузки конструкторов в зависимости от параметров(?)
- необходимость уникальных имён методов для каждого набора параметров.
- необходимость куда-то возвращать ошибки?
- отсутствие методов класса.

В общем, пораскинув мозгами, я решил, что простота Go рулит, и усложнять
это место не особо нужно.
Тем более, что зачастую объекты создаются в методах других объектах.

-----

## Объекты

Объекты - это экземпляры/инстансы классов.

### Aллокация.
```c
    hotDState*  dst;
    hotCState*  cst;

    // По умолчанию, аллоцируется, зачищенное нулями.
    dst = $alloc(DState);

    // Но можно указать значения.
    cst = $alloc(CState, .p.st = 100, .add_acc = 0.1);
```

На что нужно обратит внимание:
- если вы пользуетесь передачей начальных значений в `$alloc`
- и освобождаете что-то в `fobjDispose`
- то передавать в `$alloc` нужно то, что можно в `fobjDispose` освободить.

Т.е.
- если вы передаёте объект, то его нужно `$ref(obj)` (см.ниже)
- если вы передаёте строку, то её нужно `ft_strdup(str)`
- и т.д.

### Вызов метода

В вызове метода можно указывать аргументы по порядку или используя
имена параметров.

Опциональные параметры можно пропускать. После пропущенного опционального
параметра можно использовать только именованные параметры.

```c
    // "Классический"
    printf("%f\n", $(hotMultAndAdd, dst, 2, 3));
    printf("%f\n", $(hotMultAndAdd, cst, 3, 4));
    printf("%f\n", $(hotGetState, dst));
    printf("%f\n", $(hotGetState, cst));
    printf("%f\n", $(hotAddAndMult, cst, 5, 6));

    // С именованными параметрами
    printf("%f\n", $(hotMultAndAdd, dst, .mult = 2, .add = 3));
    printf("%f\n", $(hotMultAndAdd, cst, .add = 3, .mult = 4));
    printf("%f\n", $(hotGetState, dst)); // нет параметров.
    printf("%f\n", $(hotAddAndMult, cst, .add = 5, .mult = 6));
    printf("%f\n", $(hotAddAndMult, cst, .mult = 5, .add = 6));

    // С дефолтными параметрами
    printf("%f\n", $(hotMultAndAdd, dst, .mult = 2));
    printf("%f\n", $(hotMultAndAdd, cst, .add = 3));
    printf("%f\n", $(hotMultAndAdd, cst));
    // А вот это упадёт с FATAL, т.к. у hotAddAndMult не имеет
    // опциональных аргументов
    // printf("%f\n", $(hotAddAndMult, cst, .add = 5));
    // printf("%f\n", $(hotAddAndMult, cst, .mult = 5));
    // printf("%f\n", $(hotAddAndMult, cst));
```

Можно использовать метод непосредственно как С функцию, но аргументы
придётся тогда указывать все и по порядку. Именнованные аргументы
при этом указать не получится, и пропустить опциональные - тоже.

```c
    printf("%f\n", hotMultAndAdd(dst, 2, 3));
    printf("%f\n", hotMultAndAdd(cst, 3, 4));
    printf("%f\n", hotGetState(dst));
    printf("%f\n", hotGetState(cst));
    printf("%f\n", hotAddAndMult(cst, 5, 6));
    // а вот это свалится с FATAL
    // printf("%f\n", hotAddAndMult(dst, 6, 7));
```

### Условные вызов метода.
    
Доступна конструкция вызова метода только в случае, если он определён:

```c
    double v;
    if ($ifdef(v =, hotMultAndAdd, dst, .mult = 1)) {
        printf("dst responds to hotMultAndAdd: %f\n", v);
    }
    
    if ($ifdef(, hotGetStatus, cst)) {
        printf("cst responds to hotGetStatus.\n"
               "Result assignment could be ommitted. "
               "Although compiler could warn on this.");
    }
```

### Проверка реализации метода

Можно проверить, определён ли метод на объекте, с помощью макроса
`$implement(Method, obj)`

```c
    if ($implements(hotGetState, dst)) {
        workWithObject(dst);
    }

    hotGetState_i hgs;
    if ($implements(hotGetState, dst, &hgs)) {
        $i(hotGetState, hgs);
    }
```

(На самом деле, используется механизм определения реализации интерфейса,
сгенерённого для метода).

-------

## Интерфейс

Интерфейс - это формальный набор методов.

Служит для цели проверки согласованности реализации объекта/класса, и
улучшает самодокументируемость сигнатур методов.

Для каждого метода сразу создаётся интерфейс, содержащий один обязательный
метод. Потому создавать ещё раз интерфейс для одного метода не требуется.

Чтобы создать интерфейс, нужно:

- объявить интерфейс
```c
    #define iface__hotState mth(hotMultAndAdd, hotGetState), \
                            opt(hotAddAndMult)
```
  Здесь мы объявили два обязательных и один опциональный метод.
  Количество секций mth и opt - произвольное. Количество методов в них -
  тоже.
  (Произвольное - в пределах разумного - ~ до 16)

- позвать генерирующий макрос
```c
    fobj_iface(hotState);
```

  Макрос генерирует объявления:
```c
    // Структура интерфейса с реализациями методов.
    typedef union hotState_i {
        fobj_t      self;
        uintptr_t   has_hotMultAndAdd;
        uintptr_t   has_hotGetState;
        uintptr_t   has_hotAddAndMult;
    } hotState_i;

    // Биндинг интерфейса для объекта
    static inline hotState_i    bind_hotState(fobj_t obj);
    // Биндинг интерфейса + увеличение счётчика ссылок на объекте
    static inline hotState_i    bindref_hotState(fobj_t obj);
    // Проверка реализации интерфейса
    static inline bool  implements_hotState(fobj_t obj, hotState_i *iface);
```
  И "скрытое объявление"
```
    // Проверка объявления интерфейса
    static inline void
    fobj__klass_validate_hotState(fobj__klass_handle_t klass);
```

### Биндинг метода/интерфейса

По сути, это всегда биндинг интерфейса. Просто каждый метод определяет
интерфейс с одним этим методом.

```c
    hotMultAndAdd_i  hmad = bind_hotMultAndAdd(dst);
    hotMultAndAdd_i  hmac = bind_hotMultAndAdd(cst);
    hotState_i       hstd = bind_hotState(dst);
    hotState_i       hstc = bind_hotState(cst);
```

### Вызов метода на интерфейсе

Заметьте, тут интерфейс передаётся по значению, а не по поинтеру.
Сделал так после того, как один раз ошибся: вместо `$i()` написал `$()`,
и компилятор радостно скомпилировал, т.к. `$()` принимает `void*`.

```c
    printf("%f\n", $i(hotMultAndAdd, hmaa, .mult = 1));
    printf("%f\n", $i(hotMultAndAdd, hmac, .add = 2));

    printf("%f\n", $i(hotMultAndAdd, hstd));
    printf("%f\n", $i(hotMultAndAdd, hstc));
    printf("%f\n", $i(hotGetState, hstd));
    printf("%f\n", $i(hotGetState, hstc));

    printf("%f\n", $i(hotAddAndMult, hstc, .mult = 4, .add = 7));
    // Проверка на обязательность аргументов тут работает так же.
    // Потому след.вызовы упадут с FATAL:
    // $i(hotAddAndMult, hstd, .mult = 1);
    // $i(hotAddAndMult, hstd, .add = 1);
    // $i(hotAddAndMult, hstd);
```

A вот на `hstd` так просто `hotAddAndMult` позвать нельзя:
- `hotDState` этот метод не определял
- `hotAddAndMult` является опциональным методом интерфейса
- потому в `hstd` этот метод остался не заполненным.
Нужно проверять:

```c
    if ($ifilled(hotAddAndMult, hstd)) {
        printf("This wont be printed: %f\n",
                $i(hotAddAndMult, hstd, .mult=1, .add=2));
    }
    if (fobj_iface_filled(hotAddAndMult, hstd)) { /*...*/ }
```

Или воспользоваться условным вызовом метода:
```c
    if ($iifdef(v =, hotAddAndMult, hstd, .mult = 1, .add = 2)) {
        printf("This wont be printed: %f\n", v);
    }
```

### Проверка реализации интерфейса

Вызов `bind_someInterface` упадёт с FATAL, если интерфейс не реализован.

Проверить, реализован ли интерфейс, можно с помощью `$implements()`:

```c
    if ($implements(hotState, dst)) {
        workWithObject(dst);
    }

    if ($implements(hotState, dst, &hstd)) {
        $i(hotGetState, hstd);
    }
```

### Накладные расходы.

Интерфейс служит только для типизации, и реализован в виде union размером в 1
поинтер. В целом, накладные расходы не отличаются от поинтера на объект.

--------

## Время жизни объекта.

Время жизни объекта управляется методом подсчёта ссылок.

Когда счётчик доходит до 0, объект уничтожается (и вызывается его fobjDispose, если определён).

### Счётчик ссылок

#### Инкремент счётчика ссылок

Если объект `a` начинает ссылается на объект `b`, то счётчик ссылок `b`
должен быть увеличен на 1. Для этого служит `$ref(fobj)`/`fobj_ref(obj)`.

```c
    // Увеличить счётчик на объекте.
    
    store->field = $ref(obj);
    
    // Увеличить счётчик на объекте, возвращённом из метода.

    store->field = $ref($(createObject, fabric));

```

То же самое, когда вы аллоцируете объект и передаёте ему ссылку на другой
объект

```c
    store = $alloc(Store, .field = $ref(obj));
    
    store = $alloc(Store, .field = $ref($(createObject, fabric)));
```

#### Декремент счётчика ссылок

Если объект `a` перестаёт ссылаться на объект `b`, то нужно уменьшить
счётчик ссылок `b` на 1.

```c
    $del(&store->field);
```

То же нужно делать и в деструкторе (`fobjDispose`):

```c
    void
    Store_fobjDispose(Vself) {
        Self(Store);
        $del(&Store->field);
    }
```

#### Корректное перезапись ссылки

Если требуется переписать ссылку объекта `a` с объекта `b1` на объект
`b2`, то нужно не забыть декремент счётчика на `b1`. Но это нужно сделать
после инкремента счётчика на `b2`, т.к. это может быть один и тот же
объект.

Чтобы избежать ошибок, используйте:

```c
    $set(&store->field, obj);
    
    $set(&store->field, $(createObject, fabric));
```

Заметьте: явно звать `$ref` или `$del` *не нужно*.

### AutoRelease Pool (ARP)

Для облегчения жизни программиста, используется концепция AutoRelase Pool:
- в начале крупных функций и в теле долгих циклов нужно объявить пул.
  Также можно объявить пул для блока, если нужно ограничить время жизни
  объектов.
```c
    fobj_t
    longRunningFunction()
    {
        FOBJ_FUNC_ARP();
        /*...*/
        for (i = 0; i < 1000000000; i++) {
            FOBJ_LOOP_ARP();
            /*...*/
        }
         /*...*/
        {
            FOBJ_BLOCK_ARP();
            /*...*/
        }
    }
```
- AutoRelease Pool очищается при выходе из скоупа (функции, блока, одной
  итерации цикла). Для этого используется расширение GCC (поддержанное
  так же clang, tcc, lcc (Эльбрус), и возможно другими)
  `__attribute__((cleanup(func))`.
  При этом к каждому объекту, помещённым в ARP, применяется `$del`
  столько раз, сколько раз объект помещён в пул.

Все вновь созданные объекты имеют refcnt = 1 и уже помещены в "ближайший"
ARP. Если к новому объекту не применено `$ref`, то он будет уничтожен.

#### Разлинковка объектов

Если требуется вернуть объект `b`, с которым объект `a` теряет связь,
то нужно не делать декремент счётчика, а помещать объект в ARP.

Для этого служит `$unref(b)`:

```c
    fobj_t b = store->field;
    store->field = NULL;
    return $unref(b);
```

Для перезаписи ссылки и возврата предыдущего значения служит `$swap`.
При этом возвращаемое значение помещается в ARP:

```c
    return $swap(&store->field, b2);
```

Этим же удобно пользоваться для однострочной разлинковки:

```c
    reutrn $swap(&store->field, NULL);
```

#### Спасение объекта

Как уже говорилось, при выходе из скоупа, для которого объявлен
ARP, объект может быть уничтожен (т.к. счётчик ссылок у него станет
равным 0).

Можно "спасать" объект, вручную увеличивая счётчик ссылок с помощью
`$ref(obj)`, и потом помещая в ARP пул другого скоупа с помощью
`$unref(obj)`.

Но для удобства сделаны макросы `$save(obj)`, `$result(obj)` и
`$return(obj)`.

`$save(obj)` сохраняет объект в случае выхода из блока/цикла. Он берёт
пул, объявленный с помощью `FOBJ_BLOCK_ARP()` или `FOBJ_LOOP_ARP()`,
узнаёт из него ссылку на предыдущий, и сохраняет объект в этом предыдущем
ARP пуле (предварительно сделав инкремент счётчика ссылок).

`$result(obj)` делает то же самое, но с пулом, объявленным в скоупе
функции с помощью `FOBJ_FUNC_ARP()`. Таким образом, объект можно будет
передать в качестве результата, не опасаясь, что он будет тут же
уничтожен.

`$return(obj)` разворачивается просто в `return $result(obj)`.

#### Интерфейсы

Такие же макросы есть для работы с интерфейсами:
- `$iref(iface)`
- `$iunref(iface)`
- `$iset(&iface_var, iface)`
- `$iswap(&iface_var, iface)`
- `$idel(&iface_var)`
- `$isave(iface)`
- `$iresult(iface)`
- `$ireturn(iface)`

### Пример

#### Cвязи между объектами

```c
    typedef struct myKlass2 {
        fobj_t someObj;
        char*  someStr;
    } myKlass2;

    #define mth__setSomeObj void, (fobj_t, so)
    fobj_method(setSomeObj)
    
    #define mth__delSomeObj void
    fobj_method(delSomeObj)
    
    #define mth__popSomeObj fobj_t
    fobj_method(popSomeObj)
    
    #define mth__replaceSomeObj void, (fobj_t, so)
    fobj_method(replaceSomeObj)
     
    #define mth__setSomeStr void, (char*, ss)
    fobj_method(setSomeStr)
    
    #define kls__myKlass2 mth(fobjDispose), \
                          mth(setSomeObj, delSomeObj, popSomeObj, replaceSomeObj), \
                          mth(setSomeStr)
    fobj_klass(MyKlass2)

    /* Корректно освобождаем ресурсы */
    myKlass2_fobjDispose(VSelf) {
        Self(myKlass2);
        $del(&self->someObj);
        ft_free(self->someStr);
    }
    
    void
    myKlass2_setSomeObj(VSelf, fobj_t so) {
        Self(myKlass2);
        $set(&self->someObj, so);
    }

    void
    myKlass2_delSomeObj(VSelf) {
        Self(myKlass2);
        $del(&self->someObj);
    }

    void
    myKlass2_popSomeObj(VSelf) {
        Self(myKlass2);
        return $swap(&self->someObj, NULL);
        // Or
        // fobj_t old = self->someObj;
        // self->someObj = NULL;
        // return $unref(old);
    }

    void
    myKlass2_replaceSomeObj(VSelf, fobj_t so) {
        Self(myKlass2);
        return $swap(&self->someObj, so);
        // Or
        // fobj_t old = self->someObj;
        // self->someObj = $ref(so);
        // return $unref(old);
    }
    
    myKlass2_resetSomeObj(VSelf, fobj_t so) {
        Self(myKlass2);
        const char *old = self->someStr;
        $set(&self->someObj, so);
        self->someStr = ft_strdup(ss);
        ft_free(old);
    }
    
    myKlass2*
    make_MyKlass2(fobj_t so, char *ss) {
        return $alloc(myKlass2,
                      .someObj = $ref(so),
                      .someStr = ft_strdup(ss));
    }

    myKlass2*
    make_set_MyKlass2(fobj_t so, char *ss) {
        MyKlass2* mk = $alloc(myKlass2);
        mk->someObj = $ref(so);
        mk->someStr = ft_strdup(ss);
        return mk;
    }
```

#### Работа с ARP пулом

```c
    // Нужно вернуть объект
    fobj_t
    doSomethingAndReturn(/*...*/, fobjErr **err) {
        FOBJ_FUNC_ARP(); // AutoRelease Pool для функции
        fobj_t result;
        fobj_t loop_result = NULL;
        // Проверим, что err != NULL, и присвоим *err = NULL
        fobj_reset_err(err);

        for(/*...*/) {
            FOBJ_LOOP_ARP(); // AutoRelease Pool для каждой итерации цикла
            fobj_t some = findsomewhere(/*...*/);

            if (isGood(some)) {
                // Если не сделать $save(some), то он (возможно)
                // уничтожится при выходе из цикла.
                loop_result = $save(some);
                break;
            }
            if (tooBad(some)) {
                // нужно "вернуть" err
                *err = fobj_error("SHIT HAPPENED");
                // Без этого *err будет уничтожен при выходе из функции
                $result(*err);
                return NULL;
            }
        }

        result = createKlass(loop_result);
        $return(result);

        // Если сделать просто `return result`, то объект уничтожится
        // при выходе из функции.
    }
```

Для быстрого выхода из вложенных ARP пулов можно вручную позвать
`$ref` + `$unref`:

```c
    fobj_t
    doSomethingAndReturn(/*...*/) {
        FOBJ_FUNC_ARP(); // AutoRelease Pool для функции
        fobj_t result;
        fobj_t loop_result = NULL;

        {
            FOBJ_BLOCK_ARP();
            /*...*/
            for (/*...*/) {
                FOBJ_LOOP_ARP();
                /*...*/
                if (/*...*/) {
                    loop_result = $ref(some);
                    goto quick_exit;
                }
            }
        }
    quick_exit:
        // Не забыть поместить в ARP
        $unref(loop_result)
        /*...*/
    }
```

## Инициализация

В главном исполняемом файле где-нибудь в начале функции `main` нужно позвать:
```c
    fobj_init();
```
До этого момента создание новых классов (`fobj_klass_init`) будет падать с
FATAL ошибкой.

Метод подготавливает рантайм и определяет некоторые базовые классы и методы.
