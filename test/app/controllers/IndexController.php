<?php

class IndexController extends ControllerBase
{

	public function indexAction()
	{
		$test = Personas::findFirst();
		var_dump($test->nombres);
		$this->view->disable();
		return;

		//Store and check for errors
		$success = $test->save($this->request->getPost(), array('name', 'email'));

		if ($success) {
			echo "Thanks for register!";
		} else {
			echo "Sorry, the following problems were generated: ";
			foreach ($user->getMessages() as $message) {
				echo $message->getMessage(), "<br/>";
			}
		}


		$this->view->disable();
	}

}

