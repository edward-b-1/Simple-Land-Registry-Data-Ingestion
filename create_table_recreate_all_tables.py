
import create_table_pp_complete_data
import create_table_pp_complete_metadata


def main():

    recreate = True

    create_table_pp_complete_data.main(recreate=recreate)
    create_table_pp_complete_metadata.main(recreate=recreate)


if __name__ == '__main__':
    main()
