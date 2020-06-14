# data-warehouse

data management system for SickZil-Machine

testdb -> szmc
Use DATA1 (MyPassport) as data lake

어노테이션은 어떤 객체 입력(UUID)에 대한 객체 출력(UUID)이다. \
각 annotation 관계 해석 방식은 어노테이션 방법(annotation_method)에 따라 다르다.

이미지는 이미지 파일의 일부일 수 있다. crop된 이미지를 데이터로 쓸 수 있기 때문에.

# Tests
#### unit test(useless)
`pytest tests/old_unit`

#### integration test
`pytest tests/integration --conn id:pw@host:port/testdb` \
`pytest tests/integration --conn id:pw@host:port/testdb --root path/to/old_snet/dir --yaml path/to/yaml`
`pytest tests/integration --conn $db --root $root --yaml $yaml`

#### mypy
`mypy`
