#let title-page(body) = {
  set page(
    numbering: none,
    footer: none,
  )
  align(center, body)
}

#title-page[
  Министерство образования Республики Беларусь
  #v(0.4em)
  Учреждение образования «Белорусский государственный университет \ информатики и радиоэлектроники»
  #v(2.5em)

  Факультет компьютерных систем и сетей

  Кафедра Информатики

  Дисциплина «Модели данных и системы управления базами данных»
  #v(4em)

  #text(weight: "bold")[ОТЧЁТ]

  к лабораторной работе №6

  на тему

  #upper[#text(weight: "bold")["Основы MongoDB"]]
  // #v(1em)

  БГУИР 6-05-0612-02 23

  #v(6em)

  #align(right)[
    #block(width: 10.5cm)[
      #align(left)[Выполнил студент группы 353503]

      #align(left)[СЕБЕЛЕВ Дмитрий Юрьевич]

      #v(1.5em)

      #block(width: 88%)[
        #stack(
          dir: ttb, // Направление: сверху-вниз (top-to-bottom)
          spacing: 9pt, // Небольшой отступ между линией и текстом
          line(length: 100%),
          align(center, text(size: 8pt)[(дата, подпись студента)]),
        )
      ]
      #v(1em)

      #align(left)[Проверил ассистент каф. информатики]

      #align(left)[ИОВИЧ Евгения Александровна]

      #v(1.5em)

      #block(width: 88%)[
        #stack(
          dir: ttb, // Направление: сверху-вниз (top-to-bottom)
          spacing: 9pt, // Небольшой отступ между линией и текстом
          line(length: 100%),
          align(center, text(size: 8pt)[(дата, подпись преподавателя)]),
        )
      ]
    ]
  ]

  #v(1fr)
  Минск 2026
]
