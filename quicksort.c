/*
	Υλοποίηση quicksort που χρησιμοποιεί σταθερό αριθμό thread υλοποιημένα με κυκλική ουρά.

	compile: gcc -O2 -Wall -pthread dev_quicksort.c -o dev_quicksort
	run: dev_quicksort 1000 4
*/

#include <stdio.h>
#include <stdlib.h>
// #include <time.h>

#include <pthread.h>

// Ελάχιστο όριο για την ανάθεση της 
// ταξινόμησης σε σειριακή μέθοδο (insertion sort)
#define CUTOFF 10

// μέγεθος ουράς του buffer με τα μυνήματα
#define QUEUE_SIZE 50

/*
	Τύποι μυνημάτων που δέχεται κάθε θέση στην ουράς μηνυμάτων
*/
#define WORK 0
#define FINISH 1
#define SHUTDOWN 2

// Αριθμός των thread
int THREADS;

// μέγεθος πίνακα ταξινόμησης
int N;

// Τύπος δεδομένων για τα μηνύματα (πακέτα εργασίας) στην ουρά
struct message {
	int type;
	int start_pos;
	int end_pos;
};

// Ουρά μηνυμάτων
struct message mqueue[QUEUE_SIZE];

// Μεταβλητές που δείχνουν θέσεις στην ουρά για την 
// εισαγωγή (qin) και εξαγωγή (qout) στοιχείων.
int qin = 0, qout = 0;

// Μεταβλητή που δείχνει το τρέχον πλήθος των μηνυμάτων στην ουρά.
int message_count = 0;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;

void swap(double *a, double *b);
void inssort(double *a, int n);
int partition(double *a, int n);
// void quicksort(double *a, int n);

void send(int type, int start_pos, int end_pos) {
	pthread_mutex_lock(&mutex);
	
	// Μόλις βγούμε από το while έχουμε το mutex
	while(message_count >= QUEUE_SIZE) {
		pthread_cond_wait(&msg_out, &mutex);
	}

	// Εισαγωγή μηνύματος στην ουρά	
	mqueue[qin].type = type;
	mqueue[qin].start_pos = start_pos;
	mqueue[qin].end_pos = end_pos;

	// Αύξηση του μετρητή εισαγωγής στοιχείων στην ουρά
	qin++;

	// Έλεγχος ορίων qin ως προς το μέγεθος της ουράς.
	// Γίνεται αρχικοποίηση στο μηδέν στην περίπτωση
	// που είναι μεγαλύτερο από το μέγεθος της ουράς.
	if(qin >= QUEUE_SIZE) {
		qin = 0;
	}

	// Αύξηση μετρητή αριθμού μηνυμάτων
	message_count++;
	
	pthread_cond_signal(&msg_in);
	pthread_mutex_unlock(&mutex);
}

void recv(int *type, int *start_pos, int *end_pos) {
	pthread_mutex_lock(&mutex);
	
	while(message_count < 1) {
		pthread_cond_wait(&msg_out, &mutex);
	}
	
	// Αντιγραφή τιμών εισόδου από το τρέχον υπάρχον μήνυμα στην ουρά
	*type = mqueue[qout].type;
	*start_pos = mqueue[qout].start_pos;
	*end_pos = mqueue[qout].end_pos;

	// Αύξηση μετρητή εξαγωγής στοιχείων στην ουρά 
	qout++;

	// Έλεγχος ορίων qout ως προς το μέγεθος της ουράς.
	// Γίνεται αρχικοποίηση στο μηδέν στην περίπτωση
	// που είναι μεγαλύτερο από το μέγεθος της ουράς.
	if(qout >= QUEUE_SIZE) {
		qout = 0;
	}

	message_count--;
	
	pthread_cond_signal(&msg_out);
	pthread_mutex_unlock(&mutex);
}

void *thread_func(void *params) {
	double *a = (double*)params;
	int type, start_pos, end_pos;
    while(1) {
		recv(&type, &start_pos, &end_pos);
		if(type == SHUTDOWN) {
			send(SHUTDOWN, 0, 0);
			break;
        }
		else if(type == FINISH) {
			send(FINISH, start_pos, end_pos);
		}
		else if (type == WORK) {
			// Τρέχον μέγεθος φορτίου από την ουρά.
			int n = end_pos - start_pos;

			// Άν το μέγεθος του φορτίου είναι μικρότερο από 
			// ένα όριο τότε η ταξινόμηση γίνεται επιτόπου.
			// Αλλιώς χωρίζεται στα δύο και δημιοιυργούνται 
			// δύο νέα πακέτα προς επεξεργασία.
			if(n <= CUTOFF) {
				inssort(a+start_pos, n);
				send(FINISH, start_pos, end_pos);
			}
			else {
				int pivot = partition(a+start_pos, n);
				int middle = start_pos+pivot;
				send(WORK, start_pos, middle);
				send(WORK, middle, end_pos);
			}
        }
    }

	// Τερματισμός του τρέχοντος thread με κενή επιστρεφόμενη τιμή.
    pthread_exit(NULL);
}

int main(int argc, char **argv) {
	// Εάν δεν δωθεί ο ζητούμενος αριθμός παραμέτρων, τότε 
	// προβάλλεται μήνυμα για την ορθή χρήση κληση του προγράμματος.
	if(argc < 3) {
		printf("usage: %s <N> <THREADS>", argv[0]);
		return 1;
	}
	
	// Παράμετροι N και THREADS του χρήστη.
	// Γίνεται μετατροπή από assci σε int (atoi)
	// για την ανάθεση των μεταβλητών.
	N = atoi(argv[1]);
	THREADS = atoi(argv[2]);

	// Μεταβλητή που χρησιμεύει ως global μετρητή για δομές επαναλήψεων.
	int i = 0;

	// Δημιουργία, δέσμευση, και έλεγχος δέσμευσης πίνακα προς ταξινόμηση.
	double *a = (double*)malloc(N*sizeof(double));
	if(a == NULL) {
		return 1;
	}

	// srand(time(NULL));

	// Αρχικοποίηση του πίνακα a με τυχαίες τιμές.
	for(i=0; i<N; i++) {
		a[i] = (double)rand()/RAND_MAX;
		// a[i] = N - i;
		// printf("%.1f\t", a[i]);
	}

	// printf("\n\n");

	// Δημιουργία πίνακα με τα threads (threads)
	// Σε περίπτωση αποτυχίας αρχικοποίησης κάθε 
	// thread γίνεται διακοπή του προγράμματος
	pthread_t threads[THREADS];
	for(i=0; i<THREADS; i++) {
		if(pthread_create(&threads[i], NULL, thread_func, a) != 0) {
			printf("error: thread creation");
			free(a);
			return 1;
		}
	}
	
	// Προσθήκη πρώτου πακέτου εργασίας στην ουρά.
	// Εισαγωγή αρχής και τέλους του πίνακα.
	send(WORK, 0 , N);

	// Μεταβλητές για την τρέχουσα κατάσταση του τρέχοντος πακέτου στο main thread
	int type, start_pos, end_pos;

	// Μετρητής ελέγχου τερματισμού
	int completed = 0;
    
	// Μόλις εξυπηρετηθούν όλα τα στοιχεία του πίνακα γίνεται τερματισμός.
	while(completed < QUEUE_SIZE) {
		// Σάρωση στοχείων στην ουρά και αντιγραφή τιμών του τρέχον πακέτου.
		recv(&type, &start_pos, &end_pos);

		if(type == FINISH) {
			// Αύξηση του μετρητή κατά το πλήθος των στοιχείων του πακέτου που έχουν εξυπηρετηθεί.
            completed += end_pos - start_pos;
        }
		else {
            send(type, start_pos, end_pos);
        }
    }

	// Τερματισμός των thread στέλνοντας μήνυμα τύπου SHUTDOWN.
	send(SHUTDOWN, 0, 0);

	// join ανεπτυγμένων threads
	for(i=0; i<THREADS; i++) {
		pthread_join(threads[i], NULL);
	}

	// Έλεγχος ορθότητας ταξινόμησης του πίνακα a.
	for(i=0; i<N-1; i++) {
		if (a[i] > a[i+1]) {
			printf("error: a[%d]=%f < a[%d]=%f !!!\n", i, a[i], i+1, a[i+1]);
			break;
		}
		// printf("%.1f\t", a[i]);
	}

	// Αποδέσμευση μνήμης πίνακα a
	free(a);
	
	// Αποδέσμευση μνήμης των δομών του pthread
	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&msg_in);
	pthread_cond_destroy(&msg_out);
	
	return 0;
}

// Αντιμετάθεση τιμών δύο μεταβλητών τύπου int pointer.
void swap(double *a, double *b) {
	double tmp = *a;
	*a = *b;
	*b = tmp;
}

// Υλοποίηση αλγορίθμου insertion sort.
void inssort(double *a, int n) {
	int i,j;
	for(i=0; i<n; i++) {
		j=i;
		while((j>0)&&(a[j-1]>a[j])) {
			swap(&a[j-1], &a[j]);
			j--;
		}
	}
}

// Εύρεση καλύτερου σημέιο pivot.
int partition(double *a, int n) {
	int first = 0, middle = n/2, last = n-1;
	if(a[middle]<a[first]) {
		swap(&a[first], &a[middle]);
	}
	if(a[last]<a[middle]) {
		swap(&a[middle], &a[last]);
	}
	if(a[middle]<a[first]) {
		swap(&a[first], &a[middle]);
	}
	
	double pivot = a[middle];
	int i,j;
	for(i=1, j=n-2;; i++, j--) {
		while(a[i]<pivot) i++;
		while(a[j]>pivot) j--;
		if(i>=j) break;
		swap(&a[i], &a[j]);
	}
	return i;
}

// void quicksort(double *a, int n) {
// 	if(n<=CUTOFF) {
// 		inssort(a, n);
// 		return;
// 	}
// 	int i = partition(a, n);
// 	quicksort(a, i);
// 	quicksort(a+i, n-i);
// }
